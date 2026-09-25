package auth

import "testing"

func set(values ...string) FieldSet {
	r := FieldSet{}
	for _, value := range values {
		r[value] = struct{}{}
	}
	return r
}

func principal(t *testing.T, role string, permissions ...string) *Principal {
	t.Helper()
	p, err := newPrincipal("u-1", role, "sid-1", permissions)
	if err != nil {
		t.Fatal(err)
	}
	return &p
}

func TestPrincipalExposesReadOnlyIdentityAndCopiesPermissions(t *testing.T) {
	input := []string{"view"}
	p, err := newPrincipal("u-1", "member", "sid-1", input)
	if err != nil {
		t.Fatal(err)
	}
	input[0] = "edit"
	if p.UserID() != "u-1" || p.Role() != "member" || p.SessionID() != "sid-1" {
		t.Fatalf("principal identity getters returned unexpected values: %#v", p)
	}
	if !p.HasPermission("view") || p.HasPermission("edit") {
		t.Fatalf("principal permissions changed through constructor input: %#v", p.permissions)
	}
	if p.HasPermission("") {
		t.Fatal("empty permission unexpectedly granted")
	}
}

func TestPrincipalNilGettersAreSafe(t *testing.T) {
	var p *Principal
	if p.UserID() != "" || p.Role() != "" || p.SessionID() != "" || p.HasPermission("view") {
		t.Fatal("nil principal exposed identity or permission")
	}
}

func TestEndpointAuthorizationFailsClosed(t *testing.T) {
	a := EndpointAuthorizer{KnownRoles: map[string]struct{}{"member": {}}, KnownPermissions: map[string]struct{}{"read": {}}}
	if err := a.Authorize(nil, &EndpointRule{Role: "member"}); err == nil {
		t.Fatal("missing principal accepted")
	}
	if err := a.Authorize(principal(t, "member"), nil); err == nil {
		t.Fatal("missing rule accepted")
	}
	if err := a.Authorize(principal(t, "member"), &EndpointRule{Role: "unknown"}); err == nil {
		t.Fatal("unknown role accepted")
	}
	if err := a.Authorize(principal(t, "member"), &EndpointRule{Permission: "unknown"}); err == nil {
		t.Fatal("unknown permission accepted")
	}
	if err := a.Authorize(principal(t, "member", "read"), &EndpointRule{Permission: "read"}); err != nil {
		t.Fatal(err)
	}
	// Principal provenance is established by authentication entry points and
	// the call convention; this layer validates the server-derived value.
}

func serviceAuthorizer() ServiceAuthorizer {
	return ServiceAuthorizer{
		KnownRoles:       map[string]struct{}{"member": {}, "support": {}, "admin": {}},
		KnownPermissions: map[string]struct{}{"view": {}, "edit": {}, "handle": {}, "audit": {}},
		Resources: map[string]ResourcePolicy{
			"users": {Roles: map[string]RoleResourcePolicy{
				"member": {Operations: map[Operation]OperationPolicy{
					Read:   {Allowed: true, RequiredPermission: "view", ReadFields: set("id", "name"), FilterFields: set("id"), SortFields: set("name"), Scope: ScopeSelf, OwnerField: "id"},
					Update: {Allowed: true, RequiredPermission: "edit", WriteFields: set("name"), Scope: ScopeSelf, OwnerField: "id"},
				}},
			}},
			"calls": {Roles: map[string]RoleResourcePolicy{
				"support": {Operations: map[Operation]OperationPolicy{
					Read: {Allowed: true, RequiredPermission: "handle", ReadFields: set("id"), Scope: ScopeAssignedCustomers, SupportField: "support_id", CustomerField: "customer_id", ActiveField: "status"},
				}},
			}},
			"audit": {Roles: map[string]RoleResourcePolicy{
				"admin": {Operations: map[Operation]OperationPolicy{
					Read: {Allowed: true, RequiredPermission: "audit", ReadFields: set("id"), Scope: ScopeGlobal},
				}},
			}},
		},
	}
}

func TestServiceRejectsUnknownAndUnauthorizedFields(t *testing.T) {
	a := serviceAuthorizer()
	p := principal(t, "member", "view", "edit")
	cases := []ServiceRequest{
		{Resource: "users", Operation: Read, ReadFields: []string{"password_hash"}},
		{Resource: "users", Operation: Read, FilterFields: []string{"email"}},
		{Resource: "users", Operation: Read, SortFields: []string{"email"}},
		{Resource: "users", Operation: Update, WriteFields: []string{"role"}},
		{Resource: "users", Operation: "unknown"},
		{Resource: "unknown", Operation: Read},
	}
	for _, request := range cases {
		if _, err := a.Authorize(p, request); err == nil {
			t.Fatalf("request accepted: %#v", request)
		}
	}
	// Only fields present in this partial update are checked.
	if _, err := a.Authorize(p, ServiceRequest{Resource: "users", Operation: Update, WriteFields: []string{"name"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Authorize(nil, ServiceRequest{Resource: "users", Operation: Read}); err == nil {
		t.Fatal("service accepted missing principal")
	}
}

func TestServiceDerivesAllDataScopesFromPrincipal(t *testing.T) {
	a := serviceAuthorizer()
	p := principal(t, "member", "view", "edit")
	c, err := a.Authorize(p, ServiceRequest{Resource: "users", Operation: Read})
	if err != nil || c.Kind != ScopeSelf || c.UserID != "u-1" || c.OwnerField != "id" {
		t.Fatalf("bad self constraint: %#v, %v", c, err)
	}

	s := principal(t, "support", "handle")
	c, err = a.Authorize(s, ServiceRequest{Resource: "calls", Operation: Read})
	if err != nil || c.Kind != ScopeAssignedCustomers || c.UserID != "u-1" || c.SupportField != "support_id" || c.CustomerField != "customer_id" || c.ActiveField != "status" || c.ActiveValue != "active" {
		t.Fatalf("bad assigned constraint: %#v, %v", c, err)
	}

	c, err = a.Authorize(principal(t, "admin", "audit"), ServiceRequest{Resource: "audit", Operation: Read})
	if err != nil || c.Kind != ScopeGlobal || c.UserID != "" {
		t.Fatalf("bad global constraint: %#v, %v", c, err)
	}

	bad := serviceAuthorizer()
	bad.Resources["users"].Roles["member"].Operations[Read] = OperationPolicy{Allowed: true, RequiredPermission: "view", ReadFields: set("id")}
	if _, err := bad.Authorize(p, ServiceRequest{Resource: "users", Operation: Read}); err == nil {
		t.Fatal("missing scope accepted")
	}
}

func TestServiceDirectCallsStillRequireAuthorization(t *testing.T) {
	a := serviceAuthorizer()
	if _, err := a.Authorize(principal(t, "member", "edit"), ServiceRequest{Resource: "users", Operation: Update, WriteFields: []string{"role"}}); err == nil {
		t.Fatal("direct service call bypassed field authorization")
	}
}

func TestServiceRejectsAdminOnlyResourceForUserWithAuditPermission(t *testing.T) {
	a := serviceAuthorizer()
	if _, err := a.Authorize(principal(t, "member", "audit"), ServiceRequest{Resource: "audit", Operation: Read, ReadFields: []string{"id"}}); err == nil {
		t.Fatal("ordinary user with audit permission reached admin-only resource")
	}
}

func TestServiceUpdateRejectsPrincipalWithoutEditPermission(t *testing.T) {
	a := serviceAuthorizer()
	if _, err := a.Authorize(principal(t, "member", "view"), ServiceRequest{Resource: "users", Operation: Update, WriteFields: []string{"name"}}); err == nil {
		t.Fatal("direct service update accepted principal without edit permission")
	}
}

func TestRolesDoNotShareScopesOrFieldCapabilities(t *testing.T) {
	a := serviceAuthorizer()
	cases := []struct {
		name string
		p    *Principal
		req  ServiceRequest
	}{
		{name: "member cannot use global audit scope", p: principal(t, "member", "audit"), req: ServiceRequest{Resource: "audit", Operation: Read, ReadFields: []string{"id"}}},
		{name: "support cannot use self user scope", p: principal(t, "support", "handle"), req: ServiceRequest{Resource: "users", Operation: Read, ReadFields: []string{"id"}}},
		{name: "admin cannot use support fields", p: principal(t, "admin", "audit"), req: ServiceRequest{Resource: "audit", Operation: Read, ReadFields: []string{"customer_id"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := a.Authorize(tc.p, tc.req); err == nil {
				t.Fatalf("role unexpectedly received another role's capability: %#v", tc.req)
			}
		})
	}

	global, err := a.Authorize(principal(t, "admin", "audit"), ServiceRequest{Resource: "audit", Operation: Read, ReadFields: []string{"id"}})
	if err != nil || global.Kind != ScopeGlobal {
		t.Fatalf("admin global scope unavailable: %#v, %v", global, err)
	}
	assigned, err := a.Authorize(principal(t, "support", "handle"), ServiceRequest{Resource: "calls", Operation: Read, ReadFields: []string{"id"}})
	if err != nil || assigned.Kind != ScopeAssignedCustomers || assigned.ActiveValue != "active" {
		t.Fatalf("support assigned scope unavailable: %#v, %v", assigned, err)
	}
}
