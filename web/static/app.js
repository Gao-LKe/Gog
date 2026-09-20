(function () {
    function qs(id) { return document.getElementById(id); }

    let _ws = null;
    let _wsToken = '';
    let _wsStatus = '未连接';

    function getAuth() {
        return {
            token: localStorage.getItem('gb_token') || '',
            userId: localStorage.getItem('gb_user_id') || '',
            userName: localStorage.getItem('gb_user_name') || ''
        };
    }

    function setAuth(auth) {
        localStorage.setItem('gb_token', auth.token || '');
        localStorage.setItem('gb_user_id', auth.userId || '');
        localStorage.setItem('gb_user_name', auth.userName || '');
    }

    function clearAuth() {
        localStorage.removeItem('gb_token');
        localStorage.removeItem('gb_user_id');
        localStorage.removeItem('gb_user_name');
    }

    function authHeader() {
        const { token } = getAuth();
        return token ? { 'Authorization': 'Bearer ' + token } : {};
    }

    async function api(path, opts) {
        const res = await fetch(path, Object.assign({
            cache: 'no-store',
            headers: Object.assign({ 'Content-Type': 'application/json' }, authHeader(), (opts && opts.headers) || {})
        }, opts || {}));

        const isJson = (res.headers.get('content-type') || '').includes('application/json');
        const body = isJson ? await res.json() : await res.text();

        if (!res.ok) {
            const msg = (body && body.message) || (body && body.error) || ('HTTP ' + res.status);
            throw new Error(msg);
        }

        if (body && typeof body === 'object' && 'code' in body && body.code !== 0) {
            throw new Error(body.message || '请求失败');
        }

        return body;
    }

    function setupNav() {
        const { token } = getAuth();
        const navLogin = qs('navLogin');
        const navLogout = qs('navLogout');
        const navHome = qs('navHome');
        const navTopics = qs('navTopics');
        const navMe = qs('navMe');
        // 登录/退出按钮 & 个人中心 / 登录链接显示控制
        if (navLogin) {
            navLogin.style.display = token ? 'none' : '';
        }
        if (navMe) {
            navMe.style.display = token ? '' : 'none';
        }
        if (navLogout) {
            navLogout.style.display = token ? '' : 'none';
            if (token) {
                navLogout.onclick = function () {
                    clearAuth();
                    location.href = '/login';
                };
            } else {
                navLogout.onclick = null;
            }
        }

        // 顶部导航激活态
        const path = location.pathname || '/';

        if (navHome) {
            navHome.classList.toggle('nav-active', path === '/');
        }
        if (navTopics) {
            navTopics.classList.toggle('nav-active', path === '/topics' || path.indexOf('/topic/') === 0);
        }
        const navGameLibrary = qs('navGameLibrary');
        if (navGameLibrary) {
            navGameLibrary.classList.toggle('nav-active', path === '/game-library' || path.indexOf('/game/') === 0);
        }
        if (navMe) {
            navMe.classList.toggle('nav-active', path === '/me');
        }
    }

    function wsURL(token) {
        const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
        const sync = (location.pathname === '/me') ? '&sync=1' : '';
        return proto + '//' + location.host + '/api/v1/ws?token=' + encodeURIComponent(token) + sync;
    }

    function dispatchWSEvent(type, detail) {
        try {
            window.dispatchEvent(new CustomEvent(type, { detail: detail || {} }));
        } catch {
            // ignore
        }
    }

    function ensureWSConnected() {
        const { token } = getAuth();
        if (!token) return;

        // 已连接/连接中：不重复建连
        if (_ws && (_ws.readyState === WebSocket.OPEN || _ws.readyState === WebSocket.CONNECTING) && _wsToken === token) {
            return;
        }

        // token 变化时，先断开旧连接
        if (_ws && _wsToken && _wsToken !== token) {
            try { _ws.close(); } catch { /* ignore */ }
            _ws = null;
        }

        _wsToken = token;
        _wsStatus = '连接中...';
        dispatchWSEvent('goblog:ws-status', { status: _wsStatus });

        const ws = new WebSocket(wsURL(token));
        _ws = ws;

        ws.onopen = function () {
            _wsStatus = '已连接';
            dispatchWSEvent('goblog:ws-status', { status: _wsStatus });
        };
        ws.onclose = function () {
            _wsStatus = '已断开';
            dispatchWSEvent('goblog:ws-status', { status: _wsStatus });
        };
        ws.onerror = function () {
            _wsStatus = '连接错误';
            dispatchWSEvent('goblog:ws-status', { status: _wsStatus });
        };
        ws.onmessage = function (ev) {
            dispatchWSEvent('goblog:ws-message', { data: ev && ev.data });
        };
    }

    function getWSStatus() {
        return _wsStatus;
    }

    /** 游戏库/详情：数据库未填 coverUrl 时使用的统一默认封面（勿与 Picsum 随机图混淆） */
    var defaultGameCoverUrl = 'https://pic4.zhimg.com/v2-cad31f1efa6d4940651ebec9063fd5cb_r.jpg';

    var DEMO_LOGIN_TEL = '13800088001';
    var DEMO_LOGIN_PASSWORD = 'EvalTest123!';
    var DEMO_LOGIN_NAME = 'demo';

    function isAutoLoginEnabled() {
        if (localStorage.getItem('gb_token')) return false;
        return true;
    }

    function isUserNotFoundError(err) {
        var msg = err && err.message ? err.message : String(err || '');
        return msg.indexOf('用户不存在') >= 0 || msg.indexOf('40001') >= 0;
    }

    async function loginDemoUser() {
        var resp = await api('/api/v1/login', {
            method: 'POST',
            body: JSON.stringify({ tel: DEMO_LOGIN_TEL, password: DEMO_LOGIN_PASSWORD })
        });
        var data = resp && resp.data;
        if (!data || !data.token) return false;
        setAuth({
            token: data.token,
            userId: String(data.user_id || ''),
            userName: String(data.user_name || '')
        });
        return true;
    }

    async function createDemoUser() {
        await api('/api/v1/signup', {
            method: 'POST',
            body: JSON.stringify({
                username: DEMO_LOGIN_NAME,
                tel: DEMO_LOGIN_TEL,
                password: DEMO_LOGIN_PASSWORD
            })
        });
    }

    async function ensureDefaultLogin() {
        if (!isAutoLoginEnabled()) return;
        try {
            if (await loginDemoUser()) return;
        } catch (e) {
            if (!isUserNotFoundError(e)) return;
        }
        try {
            await createDemoUser();
        } catch (e) {
            // 并发注册或账号已存在时继续尝试登录
        }
        try {
            await loginDemoUser();
        } catch (e) {
            // 静默跳过
        }
    }

    window.GameBog = {
        qs,
        api,
        getAuth,
        setAuth,
        clearAuth,
        setupNav,
        ensureWSConnected,
        getWSStatus,
        defaultGameCoverUrl: defaultGameCoverUrl,
    };
    window.GoBlog = window.GameBog;

    document.addEventListener('DOMContentLoaded', function () {
        ensureDefaultLogin().finally(function () {
            setupNav();
            // 登录态：任意页面保持 WS 连接（用于在线状态/通知推送）
            ensureWSConnected();
        });
    });
})();
