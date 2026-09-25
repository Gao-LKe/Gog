package auth

import (
	"errors"
	"fmt"
	"strings"
)

// Principal is the server-derived identity used by both authorization layers.
//
// Its state is intentionally private. A future authentication middleware will
// create it from the validated Redis session with newPrincipal; callers can
// inspect the identity and check permissions without receiving mutable state.
type Principal struct {
	userID      string
	role        string
	permissions map[string]struct{}
	sessionID   string
}

// newPrincipal creates a Principal from authenticated session state. Keep this
// package-private until the auth middleware is ready to establish provenance.
func newPrincipal(userID, role, sessionID string, permissions []string) (Principal, error) {
	if strings.TrimSpace(userID) == "" || strings.TrimSpace(role) == "" || strings.TrimSpace(sessionID) == "" {
		return Principal{}, errors.New("principal identity is incomplete")
	}
	set := make(map[string]struct{}, len(permissions))
	for _, permission := range permissions {
		if strings.TrimSpace(permission) == "" {
			return Principal{}, errors.New("principal contains an empty permission")
		}
		set[permission] = struct{}{}
	}
	return Principal{userID: userID, role: role, sessionID: sessionID, permissions: set}, nil
}

// UserID returns the authenticated user's identifier.
func (p *Principal) UserID() string {
	if p == nil {
		return ""
	}
	return p.userID
}

// Role returns the authenticated user's role.
func (p *Principal) Role() string {
	if p == nil {
		return ""
	}
	return p.role
}

// SessionID returns the authenticated session identifier.
func (p *Principal) SessionID() string {
	if p == nil {
		return ""
	}
	return p.sessionID
}

// HasPermission reports whether the authenticated session has permission.
func (p *Principal) HasPermission(permission string) bool {
	if p == nil {
		return false
	}
	_, ok := p.permissions[permission]
	return ok
}

func validPrincipal(p *Principal) bool {
	return p != nil && p.userID != "" && p.role != "" && p.sessionID != "" && p.permissions != nil
}

// EndpointRule is an explicit Handler entry rule. Set exactly one of Role or
// Permission (or both when both checks are intended).
type EndpointRule struct {
	Role       string
	Permission string
}

// EndpointAuthorizer validates endpoint rules against server-known names.
type EndpointAuthorizer struct {
	KnownRoles       map[string]struct{}
	KnownPermissions map[string]struct{}
}

func (a EndpointAuthorizer) Authorize(p *Principal, rule *EndpointRule) error {
	if !validPrincipal(p) {
		return errors.New("missing or invalid principal")
	}
	if rule == nil || (rule.Role == "" && rule.Permission == "") {
		return errors.New("endpoint rule is required")
	}
	if rule.Role != "" {
		if !contains(a.KnownRoles, rule.Role) || p.role != rule.Role {
			return errors.New("endpoint role denied")
		}
	}
	if rule.Permission != "" {
		if !contains(a.KnownPermissions, rule.Permission) {
			return errors.New("unknown endpoint permission")
		}
		if !p.HasPermission(rule.Permission) {
			return errors.New("endpoint permission denied")
		}
	}
	if !contains(a.KnownRoles, p.role) {
		return errors.New("unknown principal role")
	}
	for permission := range p.permissions {
		if !contains(a.KnownPermissions, permission) {
			return errors.New("unknown principal permission")
		}
	}
	return nil
}

type Operation string

const (
	Create Operation = "create"
	Read   Operation = "read"
	Update Operation = "update"
	Delete Operation = "delete"
)

type ScopeKind string

const (
	ScopeSelf              ScopeKind = "self"
	ScopeGlobal            ScopeKind = "global"
	ScopeAssignedCustomers ScopeKind = "assigned_customers"
)

type FieldSet map[string]struct{}

type OperationPolicy struct {
	Allowed            bool
	RequiredPermission string
	ReadFields         FieldSet
	WriteFields        FieldSet
	FilterFields       FieldSet
	SortFields         FieldSet
	Scope              ScopeKind
	OwnerField         string // used by ScopeSelf
	SupportField       string // used by ScopeAssignedCustomers
	CustomerField      string // used by ScopeAssignedCustomers
	ActiveField        string // used by ScopeAssignedCustomers
}

type ResourcePolicy struct {
	Roles map[string]RoleResourcePolicy
}

type RoleResourcePolicy struct {
	Operations map[Operation]OperationPolicy
}

type ServiceRequest struct {
	Resource     string
	Operation    Operation
	ReadFields   []string
	WriteFields  []string
	FilterFields []string
	SortFields   []string
}

// QueryConstraint is the range predicate a Service must apply to its query.
// It intentionally contains no client-supplied owner ID.
type QueryConstraint struct {
	Kind          ScopeKind
	UserID        string
	OwnerField    string
	SupportField  string
	CustomerField string
	ActiveField   string
	ActiveValue   string
}

func (c QueryConstraint) Empty() bool { return c.Kind == "" }

// ServiceAuthorizer performs the second, service-side check. Policies are
// supplied by the calling service; this package does not protect existing
// services automatically and does not execute SQL.
type ServiceAuthorizer struct {
	Resources        map[string]ResourcePolicy
	KnownRoles       map[string]struct{}
	KnownPermissions map[string]struct{}
}

func (a ServiceAuthorizer) Authorize(p *Principal, request ServiceRequest) (QueryConstraint, error) {
	if !validPrincipal(p) {
		return QueryConstraint{}, errors.New("missing or invalid principal")
	}
	if !contains(a.KnownRoles, p.role) {
		return QueryConstraint{}, errors.New("unknown principal role")
	}
	for permission := range p.permissions {
		if !contains(a.KnownPermissions, permission) {
			return QueryConstraint{}, errors.New("unknown principal permission")
		}
	}
	resource, ok := a.Resources[request.Resource]
	if !ok || request.Resource == "" {
		return QueryConstraint{}, errors.New("unknown resource")
	}
	rolePolicy, ok := resource.Roles[p.role]
	if !ok {
		return QueryConstraint{}, errors.New("role has no resource policy")
	}
	policy, ok := rolePolicy.Operations[request.Operation]
	if !ok || !policy.Allowed {
		return QueryConstraint{}, errors.New("operation denied")
	}
	if policy.RequiredPermission == "" {
		return QueryConstraint{}, errors.New("operation permission is missing")
	}
	if !contains(a.KnownPermissions, policy.RequiredPermission) {
		return QueryConstraint{}, errors.New("unknown operation permission")
	}
	if !p.HasPermission(policy.RequiredPermission) {
		return QueryConstraint{}, errors.New("operation permission denied")
	}
	if err := checkFields("read", request.ReadFields, policy.ReadFields); err != nil {
		return QueryConstraint{}, err
	}
	if err := checkFields("write", request.WriteFields, policy.WriteFields); err != nil {
		return QueryConstraint{}, err
	}
	if err := checkFields("filter", request.FilterFields, policy.FilterFields); err != nil {
		return QueryConstraint{}, err
	}
	if err := checkFields("sort", request.SortFields, policy.SortFields); err != nil {
		return QueryConstraint{}, err
	}
	return deriveConstraint(p, policy)
}

func deriveConstraint(p *Principal, policy OperationPolicy) (QueryConstraint, error) {
	switch policy.Scope {
	case ScopeSelf:
		if policy.OwnerField == "" {
			return QueryConstraint{}, errors.New("self scope is incomplete")
		}
		return QueryConstraint{Kind: ScopeSelf, UserID: p.userID, OwnerField: policy.OwnerField}, nil
	case ScopeGlobal:
		return QueryConstraint{Kind: ScopeGlobal}, nil
	case ScopeAssignedCustomers:
		if policy.SupportField == "" || policy.CustomerField == "" || policy.ActiveField == "" {
			return QueryConstraint{}, errors.New("assigned scope is incomplete")
		}
		// The active status is a server policy value. It is never supplied by a
		// request, so services can apply this predicate without client SQL input.
		return QueryConstraint{Kind: ScopeAssignedCustomers, UserID: p.userID, SupportField: policy.SupportField, CustomerField: policy.CustomerField, ActiveField: policy.ActiveField, ActiveValue: "active"}, nil
	default:
		return QueryConstraint{}, fmt.Errorf("unknown or missing data scope")
	}
}

func checkFields(kind string, actual []string, allowed FieldSet) error {
	for _, field := range actual {
		if field == "" {
			return fmt.Errorf("%s field is empty", kind)
		}
		if !contains(allowed, field) {
			return fmt.Errorf("%s field denied: %s", kind, field)
		}
	}
	return nil
}

func contains(set map[string]struct{}, value string) bool { _, ok := set[value]; return ok }
