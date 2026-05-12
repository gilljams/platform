// Platform Shell v2 — Injected header for all products
// Usage: <script src="http://platform:3000/shell.js" data-platform-url="http://platform:3000"></script>
// The data-platform-url attribute is optional — auto-detected from script src if omitted.

(function () {
  "use strict";
  var SHELL_VERSION = 2;

  // ── Error isolation: if anything fails, the host product must not break ──
  try {

  // ── Configuration: auto-detect platform URL from script tag or data attribute ──
  var PLATFORM_URL = (function() {
    var scripts = document.querySelectorAll("script[src*='shell.js']");
    for (var i = 0; i < scripts.length; i++) {
      var s = scripts[i];
      // Prefer explicit data attribute
      if (s.getAttribute("data-platform-url")) return s.getAttribute("data-platform-url").replace(/\/$/, "");
      // Fall back to script src origin
      try { return new URL(s.src).origin; } catch(e) {}
    }
    return window.location.origin; // last resort
  })();

  function getCookie(name) {
    const match = document.cookie.match(new RegExp("(^| )" + name + "=([^;]+)"));
    return match ? decodeURIComponent(match[2]) : null;
  }

  function parseJwt(token) {
    try {
      const base64 = token.split(".")[1].replace(/-/g, "+").replace(/_/g, "/");
      return JSON.parse(atob(base64));
    } catch {
      return null;
    }
  }

  // Check auth
  const token = getCookie("platform_token");
  const user = token ? parseJwt(token) : null;

  if (!user) {
    window.location.href = PLATFORM_URL + "/login.html";
    return;
  }

  // Fetch all shell data in a single call (navigation, inbox, config, help, user)
  var _bootstrapData = null;
  async function loadBootstrap() {
    try {
      var res = await fetch(PLATFORM_URL + "/api/shell/bootstrap", { credentials: "include" });
      if (!res.ok) return null;
      _bootstrapData = await res.json();
      return _bootstrapData;
    } catch { return null; }
  }

  // Legacy fallback: if bootstrap endpoint is unavailable, fall back to individual calls
  async function loadNavigation() {
    try {
      var res = await fetch(PLATFORM_URL + "/api/navigation", { credentials: "include" });
      if (!res.ok) return { items: [], externalTools: [] };
      var data = await res.json();
      if (Array.isArray(data)) return { items: data, externalTools: [] };
      return { items: data.items || [], externalTools: data.externalTools || [] };
    } catch { return { items: [], externalTools: [] }; }
  }

  // Determine which product we're currently on
  function detectCurrentProduct(navItems) {
    const currentOrigin = window.location.origin;
    const currentPath = window.location.pathname;
    for (var i = 0; i < navItems.length; i++) {
      var item = navItems[i];
      // Platform admin: same origin + /admin.html path
      if (item.key === "platform" && currentOrigin === PLATFORM_URL && currentPath.includes("admin")) return item.key;
      // Other products: match by origin from their task_base_url
      if (item.key !== "platform" && item.url) {
        try {
          var itemOrigin = new URL(item.url).origin;
          if (currentOrigin === itemOrigin) return item.key;
        } catch {}
      }
    }
    return null;
  }

  // Entitlement check helper
  function resolveUrl(item) {
    // Relative URLs are on the platform itself
    if (item.url.startsWith("/")) return PLATFORM_URL + item.url;
    return item.url;
  }

  // Inject styles — light, slim shell header with pin/unpin support
  const style = document.createElement("style");
  style.textContent = `
    .platform-shell-header {
      position: fixed;
      top: 0;
      left: 0;
      right: 0;
      height: 32px;
      background: #fff;
      color: #333;
      display: flex;
      align-items: center;
      padding: 0 20px;
      font-family: Inter, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
      font-size: 12px;
      z-index: 99999;
      box-shadow: 0 1px 0 #e0e0e0;
      transition: transform 0.25s ease, opacity 0.2s ease;
    }
    .platform-shell-header.unpinned {
      transform: translateY(-100%);
      opacity: 0;
    }
    .platform-shell-header.unpinned.peek {
      transform: translateY(0);
      opacity: 1;
    }
    .platform-shell-header .logo {
      font-weight: 700;
      font-size: 12px;
      margin-right: 24px;
      color: #0d1822;
      letter-spacing: 0.2px;
      display: inline-flex;
      align-items: center;
    }
    .platform-shell-header .logo img {
      height: 22px;
      max-width: 180px;
      object-fit: contain;
    }
    .platform-shell-header nav {
      display: flex;
      gap: 2px;
    }
    .platform-shell-header nav a {
      color: #888;
      text-decoration: none;
      padding: 4px 12px;
      border-radius: 3px;
      font-weight: 500;
      font-size: 11px;
      transition: all 0.15s;
    }
    .platform-shell-header nav a:hover {
      color: #333;
      background: #f0f0f0;
    }
    .platform-shell-header nav a.active {
      color: #fff;
      background: #1f64a4;
    }
    /* ── External tool links ── */
    .shell-nav-divider {
      display: inline-block;
      width: 1px;
      height: 14px;
      background: #d0d0d0;
      margin: 0 6px;
      vertical-align: middle;
    }
    .platform-shell-header nav .shell-ext-link {
      color: #888;
      font-weight: 500;
      font-size: 11px;
    }
    .platform-shell-header nav .shell-ext-link:hover {
      color: #555;
      background: #f0f0f0;
    }
    .ext-icon { width: 9px; height: 9px; margin-left: 3px; vertical-align: middle; opacity: 0.5; }
    .shell-ext-more {
      display: inline-flex;
      align-items: center;
      padding: 2px 8px;
      border-radius: 3px;
      color: #888;
      font-size: 11px;
      font-weight: 500;
      cursor: pointer;
      transition: all 0.15s;
      position: relative;
    }
    .shell-ext-more:hover { color: #555; background: #f0f0f0; }
    .shell-ext-dropdown {
      display: none;
      position: absolute;
      top: 28px;
      background: #fff;
      border-radius: 4px;
      box-shadow: 0 4px 16px rgba(0,0,0,0.15);
      border: 1px solid #e0e0e0;
      min-width: 180px;
      z-index: 100010;
      padding: 4px 0;
    }
    .shell-ext-dropdown.open { display: block; }
    .shell-ext-dropdown a {
      display: flex;
      align-items: center;
      padding: 6px 14px;
      color: #555;
      font-size: 12px;
      font-weight: 500;
      text-decoration: none;
      transition: all 0.15s;
    }
    .shell-ext-dropdown a:hover { background: #f5f5f5; color: #333; }
    .platform-shell-header .spacer { flex: 1; }
    .shell-pin-btn {
      background: none;
      border: none;
      cursor: pointer;
      padding: 2px 4px;
      border-radius: 3px;
      line-height: 1;
      color: #bbb;
      transition: all 0.15s;
      margin-left: 8px;
      display: flex;
      align-items: center;
    }
    .shell-pin-btn svg { width: 16px; height: 16px; }
    .shell-pin-btn:hover { color: #666; background: #f0f0f0; }
    .shell-pin-btn.pinned { color: #555; }
    .shell-help-btn {
      background: none;
      border: 1px solid #ddd;
      cursor: pointer;
      padding: 3px 7px;
      border-radius: 3px;
      line-height: 1;
      color: #666;
      transition: all 0.15s;
      margin-left: 6px;
      display: inline-flex;
      align-items: center;
      font-family: inherit;
    }
    .shell-help-btn svg { width: 14px; height: 14px; vertical-align: middle; }
    .shell-help-btn:hover { color: #1f64a4; border-color: #93c5fd; background: #eff6ff; }
    .shell-help-btn.active { color: #1f64a4; border-color: #93c5fd; background: #eff6ff; }
    .shell-help-panel {
      position: fixed;
      top: 38px;
      right: 12px;
      width: 380px;
      max-height: calc(100vh - 60px);
      background: #fff;
      border-radius: 10px;
      box-shadow: 0 8px 32px rgba(0,0,0,0.18), 0 2px 8px rgba(0,0,0,0.08);
      z-index: 100001;
      display: none;
      flex-direction: column;
      overflow: hidden;
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    }
    .shell-help-panel.open { display: flex; }
    .shell-help-panel-header {
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 12px 16px;
      border-bottom: 1px solid #eee;
      font-weight: 600;
      font-size: 13px;
      color: #333;
    }
    .shell-help-panel-header svg { width: 16px; height: 16px; color: #1f64a4; }
    .shell-help-panel-body {
      flex: 1;
      overflow-y: auto;
      padding: 12px 16px;
      font-size: 13px;
      line-height: 1.6;
    }
    .shell-help-panel-body .help-search {
      width: 100%;
      padding: 8px 12px;
      border: 1px solid #ddd;
      border-radius: 6px;
      font-size: 13px;
      margin-bottom: 12px;
    }
    .shell-help-panel-body .help-search:focus { outline: none; border-color: #93c5fd; }
    .shell-help-panel-body .help-cat {
      font-size: 11px;
      color: #888;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.3px;
      margin: 12px 0 4px;
      cursor: pointer;
      user-select: none;
      display: flex;
      align-items: center;
      gap: 4px;
    }
    .shell-help-panel-body .help-cat:hover { color: #555; }
    .shell-help-panel-body .help-cat-toggle { font-size: 9px; transition: transform 0.15s; display: inline-block; }
    .shell-help-panel-body .help-cat-toggle.open { transform: rotate(90deg); }
    .shell-help-panel-body .help-group { padding-left: 12px; }
    .shell-help-panel-body .help-group.collapsed { display: none; }
    .shell-help-panel-body .help-item {
      padding: 6px 10px;
      cursor: pointer;
      border-radius: 5px;
      font-size: 13px;
      margin: 2px 0;
      display: flex;
      align-items: center;
      gap: 6px;
    }
    .shell-help-panel-body .help-item:hover { background: #f0f7ff; }
    .shell-help-panel-body .help-item .help-prod-badge {
      font-size: 9px;
      background: #f0f4ff;
      color: #1f64a4;
      border-radius: 3px;
      padding: 1px 4px;
    }
    .shell-help-panel-body .help-article-view h1 { font-size: 18px; margin: 0 0 12px; color: #222; }
    .shell-help-panel-body .help-article-view h2 { font-size: 15px; margin: 14px 0 6px; color: #333; }
    .shell-help-panel-body .help-article-view h3 { font-size: 13px; margin: 10px 0 4px; color: #444; }
    .shell-help-panel-body .help-article-view code { background: #f4f4f4; padding: 1px 4px; border-radius: 3px; font-size: 11px; }
    .shell-help-panel-body .help-article-view ul { margin: 4px 0 8px 0; padding-left: 20px; list-style: disc; }
    .shell-help-panel-body .help-article-view li { margin: 3px 0; list-style: disc; }
    .shell-help-back-btn {
      background: none;
      border: none;
      cursor: pointer;
      color: #1f64a4;
      font-size: 12px;
      padding: 4px 0;
      margin-bottom: 8px;
      display: inline-flex;
      align-items: center;
      gap: 4px;
    }
    .shell-help-back-btn:hover { text-decoration: underline; }
    .shell-ai-btn {
      background: none;
      border: 1px solid #ddd;
      cursor: pointer;
      padding: 3px 7px;
      border-radius: 3px;
      line-height: 1;
      color: #666;
      transition: all 0.15s;
      margin-left: 6px;
      display: inline-flex;
      align-items: center;
      font-family: inherit;
    }
    .shell-ai-btn svg { width: 14px; height: 14px; vertical-align: middle; }
    .shell-ai-btn:hover { color: #7c3aed; border-color: #c4b5fd; background: #f5f3ff; }
    .shell-ai-btn.active { color: #7c3aed; border-color: #c4b5fd; background: #f5f3ff; }
    .shell-ai-panel {
      position: fixed;
      top: 38px;
      right: 12px;
      width: 360px;
      max-height: calc(100vh - 60px);
      background: #fff;
      border-radius: 10px;
      box-shadow: 0 8px 32px rgba(0,0,0,0.18), 0 2px 8px rgba(0,0,0,0.08);
      z-index: 100001;
      display: none;
      flex-direction: column;
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    }
    .shell-ai-panel.open { display: flex; }
    .shell-ai-panel-header {
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 12px 16px;
      border-bottom: 1px solid #f0f0f0;
      font-size: 13px;
      font-weight: 600;
      color: #333;
    }
    .shell-ai-panel-header svg { width: 16px; height: 16px; color: #7c3aed; }
    .shell-ai-panel-body {
      flex: 1;
      overflow-y: auto;
      padding: 16px;
      font-size: 13px;
      color: #555;
      min-height: 200px;
      max-height: calc(100vh - 160px);
    }
    .shell-ai-panel-body .ai-msg { margin-bottom: 12px; line-height: 1.5; }
    .shell-ai-panel-body .ai-msg.assistant { background: #f5f3ff; border-radius: 8px; padding: 10px 12px; }
    .shell-ai-panel-body .ai-msg.user { background: #f0f9ff; border-radius: 8px; padding: 10px 12px; text-align: right; }
    .shell-ai-panel-input {
      display: flex;
      gap: 8px;
      padding: 12px 16px;
      border-top: 1px solid #f0f0f0;
    }
    .shell-ai-panel-input input {
      flex: 1;
      border: 1px solid #e2e8f0;
      border-radius: 6px;
      padding: 8px 12px;
      font-size: 13px;
      outline: none;
    }
    .shell-ai-panel-input input:focus { border-color: #7c3aed; }
    .shell-ai-panel-input button {
      background: #7c3aed;
      color: #fff;
      border: none;
      border-radius: 6px;
      padding: 8px 12px;
      font-size: 12px;
      font-weight: 600;
      cursor: pointer;
    }
    .shell-ai-panel-input button:hover { background: #6d28d9; }
    .shell-hover-zone {
      position: fixed;
      top: 0;
      left: 0;
      right: 0;
      height: 6px;
      z-index: 99998;
      display: none;
      cursor: pointer;
    }
    .shell-hover-zone.active {
      display: block;
      background: linear-gradient(to right, transparent 20%, rgba(31,100,164,0.25) 50%, transparent 80%);
      height: 3px;
      transition: height 0.2s ease, background 0.2s ease;
    }
    .shell-hover-zone.active:hover {
      height: 6px;
      background: linear-gradient(to right, transparent 10%, rgba(31,100,164,0.45) 50%, transparent 90%);
    }

    /* ── Inbox button (in shell bar) ── */
    .shell-inbox-btn {
      position: relative;
      background: none;
      border: 1px solid #ddd;
      color: #666;
      padding: 3px 10px;
      border-radius: 3px;
      cursor: pointer;
      font-size: 11px;
      transition: all 0.15s;
      display: inline-flex;
      align-items: center;
      gap: 5px;
      font-family: inherit;
      margin-right: 10px;
    }
    .shell-inbox-btn:hover { color: #333; border-color: #bbb; background: #f5f5f5; }
    .shell-inbox-btn svg { vertical-align: middle; }
    .shell-inbox-badge {
      position: absolute;
      top: -5px; right: -5px;
      background: #1f64a4;
      color: #fff;
      font-size: 9px;
      font-weight: 700;
      min-width: 15px;
      height: 15px;
      line-height: 15px;
      text-align: center;
      border-radius: 3px;
      padding: 0 3px;
    }

    /* ── Notch pill — visible when unpinned, mirrors shell bar right side ── */
    .shell-notch-pill {
      position: fixed;
      top: 0;
      right: 0;
      height: 32px;
      z-index: 100001;
      display: none;
      align-items: stretch;
      gap: 0;
      padding: 0;
      background: #0d1822;
      color: #555;
      border-radius: 0;
      font-family: Inter, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
      font-size: 11px;
      font-weight: 600;
      transition: transform 0.25s ease, opacity 0.2s ease;
      transform: translateY(0);
    }
    .shell-notch-pill .notch-content {
      display: flex;
      align-items: center;
      background: #fff;
      border-radius: 0 0 10px 10px;
      padding: 0 6px;
      box-shadow: 0 2px 8px rgba(0,0,0,0.10), -2px 0 6px rgba(0,0,0,0.04);
    }
    .shell-notch-pill .notch-inbox-btn {
      position: relative;
      background: none;
      border: 1px solid #ddd;
      color: #666;
      padding: 3px 10px;
      border-radius: 3px;
      cursor: pointer;
      font-size: 11px;
      transition: all 0.15s;
      display: inline-flex;
      align-items: center;
      gap: 5px;
      font-family: inherit;
      margin-left: 12px;
    }
    .shell-notch-pill .notch-inbox-btn:hover { color: #333; border-color: #bbb; background: #f5f5f5; }
    .shell-notch-pill .notch-inbox-btn svg { width: 14px; height: 14px; vertical-align: middle; }
    .shell-notch-pill .notch-count {
      position: absolute;
      top: -5px; right: -5px;
      background: #1f64a4;
      color: #fff;
      font-size: 9px;
      font-weight: 700;
      min-width: 15px;
      height: 15px;
      line-height: 15px;
      text-align: center;
      border-radius: 3px;
      padding: 0 3px;
    }
    .shell-notch-pill .notch-pin-btn {
      background: none;
      border: none;
      cursor: pointer;
      padding: 2px 4px;
      border-radius: 3px;
      line-height: 1;
      color: #bbb;
      transition: all 0.15s;
      margin-left: 8px;
      display: flex;
      align-items: center;
    }
    .shell-notch-pill .notch-pin-btn svg { width: 16px; height: 16px; }
    .shell-notch-pill .notch-pin-btn:hover { color: #666; background: #f0f0f0; }
    .shell-notch-pill .notch-tail {
      width: 18px;
    }
    .shell-notch-pill.visible { display: flex; }

    /* ── Inbox dropdown ── */
    .shell-inbox-dropdown {
      display: none;
      position: absolute;
      top: 30px;
      right: 0;
      width: 340px;
      max-height: 400px;
      overflow-y: auto;
      background: #fff;
      border-radius: 3px;
      box-shadow: 0 4px 20px rgba(0,0,0,0.15);
      border: 1px solid #e0e0e0;
      z-index: 100000;
      font-size: 12px;
      color: #1a1a1a;
    }
    .shell-inbox-dropdown.open { display: block; }
    /* When opened from the notch pill, position differently */
    .shell-notch-pill .shell-inbox-dropdown {
      position: fixed;
      top: 34px;
      right: 20px;
    }
    .shell-inbox-dropdown .inbox-header {
      padding: 10px 14px;
      font-weight: 700;
      font-size: 13px;
      border-bottom: 1px solid #eee;
      display: flex;
      justify-content: space-between;
      align-items: center;
    }
    .shell-inbox-dropdown .inbox-empty {
      padding: 20px 14px;
      color: #999;
      text-align: center;
    }
    .shell-inbox-item {
      padding: 8px 14px;
      border-bottom: 1px solid #f5f5f5;
      cursor: pointer;
      transition: background 0.15s;
      display: flex;
      gap: 8px;
      align-items: flex-start;
    }
    .shell-inbox-item:hover { background: #f8f9fa; }
    .shell-inbox-item.done { opacity: 0.5; }
    .shell-inbox-item .inbox-dot {
      width: 6px; height: 6px;
      border-radius: 50%;
      background: #1f64a4;
      margin-top: 5px;
      flex-shrink: 0;
    }
    .shell-inbox-item.done .inbox-dot { background: #ccc; }
    .shell-inbox-item .inbox-text { flex: 1; }
    .shell-inbox-item .inbox-title { font-weight: 600; margin-bottom: 1px; font-size: 12px; }
    .shell-inbox-item .inbox-desc { font-size: 11px; color: #888; }
    .shell-inbox-item .inbox-source {
      font-size: 9px;
      text-transform: uppercase;
      font-weight: 600;
      color: #1f64a4;
      background: #e8f0fa;
      padding: 1px 5px;
      border-radius: 3px;
      flex-shrink: 0;
    }
    .shell-inbox-item .inbox-complete-btn {
      background: none; border: 1px solid #ced4da; border-radius: 3px;
      color: #888; cursor: pointer; padding: 2px 6px; font-size: 10px;
      font-weight: 600; transition: all 0.15s; flex-shrink: 0;
      font-family: inherit; line-height: 1.3;
    }
    .shell-inbox-item .inbox-complete-btn:hover { background: #e8f5e9; color: #2e7d32; border-color: #2e7d32; }
    .shell-inbox-item.done .inbox-complete-btn { display: none; }

    /* ── User info injected into sidebar ── */
    .shell-user-info {
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 10px 18px;
      border-bottom: 1px solid rgba(255,255,255,0.1);
      margin-bottom: 6px;
    }
    .shell-user-info .user-avatar {
      width: 28px; height: 28px;
      border-radius: 50%;
      background: #1f64a4;
      color: #fff;
      font-size: 11px;
      font-weight: 700;
      display: flex;
      align-items: center;
      justify-content: center;
      flex-shrink: 0;
    }
    .shell-user-info .user-details {
      display: flex;
      flex-direction: column;
      overflow: hidden;
    }
    .shell-user-info .user-name {
      font-size: 12px;
      font-weight: 600;
      color: rgba(255,255,255,0.9);
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .shell-user-info .user-role {
      font-size: 10px;
      color: rgba(255,255,255,0.4);
    }

    body {
      padding-top: 0;
      --shell-height: 0px;
    }
  `;
  document.head.appendChild(style);

  // Hover zone — triggers shell reveal when unpinned
  const hoverZone = document.createElement("div");
  hoverZone.className = "shell-hover-zone";
  document.body.prepend(hoverZone);

  // Inject header skeleton immediately (nav populated async)
  const header = document.createElement("div");
  header.className = "platform-shell-header";
  header.innerHTML = `
    <div class="shell-inner" style="display:flex;align-items:center;width:100%">
      <span class="logo" id="platform-shell-logo" title="Platform Shell — shared navigation injected into all products via shell.js. Products remain independent; the shell provides unified navigation, inbox and external tools.">Platform POC</span>
      <nav id="platform-shell-nav"><span style="color:#aaa;font-weight:500;font-size:11px">Loading...</span></nav>
      <div class="spacer"></div>
      <div style="position:relative">
        <button class="shell-inbox-btn" id="platform-inbox-btn" title="Master Inbox — aggregated tasks from all products. Items arrive when a product assigns work (e.g. budget approval). Click an item to navigate directly to its source product.">
          <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 11l3 3l8 -8"></path><path d="M20 12v6a2 2 0 0 1 -2 2h-12a2 2 0 0 1 -2 -2v-12a2 2 0 0 1 2 -2h9"></path></svg>
          Inbox <span class="shell-inbox-badge" id="platform-inbox-count" style="display:none">0</span>
        </button>
        <div class="shell-inbox-dropdown" id="platform-inbox-dropdown">
          <div class="inbox-header">
            <span>Inbox</span>
            <span id="platform-inbox-summary" style="font-size:11px;color:#999;font-weight:400"></span>
          </div>
          <div id="platform-inbox-list">
            <div class="inbox-empty">No items</div>
          </div>
        </div>
      </div>
      <button class="shell-help-btn" id="platform-help-btn" title="Help — search product documentation and guides."><svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg></button>
      <button class="shell-ai-btn" id="platform-ai-btn" title="AI Assistant — ask questions about your data, get help with tasks, or explore platform capabilities."><svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3l1.912 5.813a2 2 0 0 0 1.275 1.275L21 12l-5.813 1.912a2 2 0 0 0-1.275 1.275L12 21l-1.912-5.813a2 2 0 0 0-1.275-1.275L3 12l5.813-1.912a2 2 0 0 0 1.275-1.275L12 3z"/><path d="M19 2l.5 1.5L21 4l-1.5.5L19 6l-.5-1.5L17 4l1.5-.5L19 2z"/></svg></button>
      <button class="shell-pin-btn" id="platform-pin-btn" title="Pin/unpin the shell bar. When unpinned, the bar hides to give products more space. Hover the top edge or use the notch pill to access inbox and navigation."><svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M15 4.5l-4 4l-4 1.5l-1.5 1.5l7 7l1.5 -1.5l1.5 -4l4 -4"/><line x1="9" y1="15" x2="4.5" y2="19.5"/><line x1="14.5" y1="4" x2="20" y2="9.5"/></svg></button>
    </div>
  `;
  document.body.prepend(header);

  // ── AI Chat Panel ──
  var aiPanel = document.createElement("div");
  aiPanel.className = "shell-ai-panel";
  aiPanel.id = "platform-ai-panel";
  aiPanel.innerHTML = `
    <div class="shell-ai-panel-header">
      <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor" stroke="none"><path d="M16 18a2 2 0 0 1 2 2a2 2 0 0 1 2 -2a2 2 0 0 1 -2 -2a2 2 0 0 1 -2 2m0 -12a2 2 0 0 1 2 2a2 2 0 0 1 2 -2a2 2 0 0 1 -2 -2a2 2 0 0 1 -2 2m-7 12a6 6 0 0 1 6 -6a6 6 0 0 1 -6 -6a6 6 0 0 1 -6 6a6 6 0 0 1 6 6"></path></svg>
      AI Assistant
      <span style="flex:1"></span>
      <button id="platform-ai-close" style="background:none;border:none;cursor:pointer;color:#999;font-size:16px;padding:2px 6px;">&times;</button>
    </div>
    <div class="shell-ai-panel-body" id="platform-ai-messages">
      <div class="ai-msg assistant">Hi! I'm the platform AI assistant. I can help you explore data, understand configurations, or answer questions about your integration setup. What would you like to know?</div>
    </div>
    <div class="shell-ai-panel-input">
      <input type="text" id="platform-ai-input" placeholder="Ask something...">
      <button id="platform-ai-send">Send</button>
    </div>
  `;
  document.body.appendChild(aiPanel);

  // Wire AI panel events (CSP-safe — no inline handlers)
  document.getElementById("platform-ai-close").addEventListener("click", function() {
    document.getElementById("platform-ai-panel").classList.remove("open");
    document.getElementById("platform-ai-btn").classList.remove("active");
  });
  document.getElementById("platform-ai-send").addEventListener("click", function() { shellAiSend(); });
  document.getElementById("platform-ai-input").addEventListener("keydown", function(e) {
    if (e.key === "Enter") document.getElementById("platform-ai-send").click();
  });

  // ── Help Panel ──
  var helpPanel = document.createElement("div");
  helpPanel.className = "shell-help-panel";
  helpPanel.id = "platform-help-panel";
  helpPanel.innerHTML = `
    <div class="shell-help-panel-header">
      <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>
      Help
      <span style="flex:1"></span>
      <button id="platform-help-close" style="background:none;border:none;cursor:pointer;color:#999;font-size:16px;padding:2px 6px;">&times;</button>
    </div>
    <div class="shell-help-panel-body" id="platform-help-body">
      <input type="text" class="help-search" id="platform-help-search" placeholder="Search help articles...">
      <div id="platform-help-list"></div>
    </div>
  `;
  document.body.appendChild(helpPanel);

  // Wire help panel events (CSP-safe — no inline handlers)
  document.getElementById("platform-help-close").addEventListener("click", function() {
    document.getElementById("platform-help-panel").classList.remove("open");
    document.getElementById("platform-help-btn").classList.remove("active");
  });
  document.getElementById("platform-help-search").addEventListener("input", function() {
    shellHelpSearch(this.value);
  });

  // Help button toggle
  var helpBtn = document.getElementById("platform-help-btn");
  helpBtn.addEventListener("click", function() {
    var panel = document.getElementById("platform-help-panel");
    var isOpen = panel.classList.toggle("open");
    helpBtn.classList.toggle("active", isOpen);
    if (isOpen) {
      document.getElementById("platform-help-search").focus();
      shellHelpLoadArticles();
    }
  });

  // Close help panel on outside click
  document.addEventListener("click", function(e) {
    var panel = document.getElementById("platform-help-panel");
    if (!panel.classList.contains("open")) return;
    if (panel.contains(e.target) || helpBtn.contains(e.target)) return;
    panel.classList.remove("open");
    helpBtn.classList.remove("active");
  });

  // Help panel logic
  var shellHelpArticles = [];
  var shellHelpCurrentSlug = null;

  function shellHelpLoadArticles() {
    // Use bootstrap data if available (saves an HTTP request)
    if (_bootstrapData && _bootstrapData.help) {
      shellHelpArticles = _bootstrapData.help;
      shellHelpRenderList(_bootstrapData.help);
      return;
    }
    var products = (user && user.products) ? user.products.join(",") : "";
    var role = (user && user.role) ? user.role : "";
    fetch(PLATFORM_URL + "/api/help/user?products=" + encodeURIComponent(products) + "&role=" + encodeURIComponent(role), { credentials: "include" })
      .then(function(r) { return r.json(); })
      .then(function(data) {
        shellHelpArticles = data;
        shellHelpRenderList(data);
      })
      .catch(function() {
        document.getElementById("platform-help-list").innerHTML = '<div style="color:#999;padding:20px;text-align:center;">Could not load help articles</div>';
      });
  }

  function shellHelpRenderList(articles) {
    var listEl = document.getElementById("platform-help-list");
    if (!articles || articles.length === 0) {
      listEl.innerHTML = '<div style="color:#999;padding:20px;text-align:center;">No articles available</div>';
      return;
    }
    // Build nested tree from "/"-separated categories
    var tree = {};
    articles.forEach(function(a) {
      var parts = (a.category || "General").split("/").map(function(s) { return s.trim(); });
      var cur = tree;
      var target;
      parts.forEach(function(p) {
        if (!cur[p]) cur[p] = { _articles: [], _children: {} };
        target = cur[p];
        cur = target._children;
      });
      target._articles.push(a);
    });

    function renderNode(obj, depth) {
      var html = '';
      var keys = Object.keys(obj).sort();
      keys.forEach(function(k) {
        if (k.charAt(0) === '_') return;
        var node = obj[k];
        var hasContent = node._articles.length > 0 || Object.keys(node._children).length > 0;
        if (!hasContent) return;
        var id = 'help-grp-' + Math.random().toString(36).substr(2, 6);
        html += '<div class="help-cat" onclick="event.stopPropagation();var g=document.getElementById(\'' + id + '\');var t=this.querySelector(\'span\');if(g.classList.toggle(\'collapsed\')){t.classList.remove(\'open\')}else{t.classList.add(\'open\')}" style="padding-left:' + (depth * 12) + 'px"><span class="help-cat-toggle open">▶</span>' + k + '</div>';
        html += '<div class="help-group" id="' + id + '">';
        node._articles.forEach(function(a) {
          var badge = a.product ? '<span class="help-prod-badge">' + a.product + '</span>' : '';
          html += '<div class="help-item" style="padding-left:' + ((depth + 1) * 12) + 'px" onclick="event.stopPropagation();shellHelpOpenArticle(\'' + a.slug + '\')">' + a.title + badge + '</div>';
        });
        html += renderNode(node._children, depth + 1);
        html += '</div>';
      });
      return html;
    }
    listEl.innerHTML = renderNode(tree, 0);
  }

  window.shellHelpSearch = function(q) {
    if (!q) {
      shellHelpRenderList(shellHelpArticles);
      return;
    }
    var lower = q.toLowerCase();
    var filtered = shellHelpArticles.filter(function(a) {
      return a.title.toLowerCase().indexOf(lower) >= 0 ||
        (a.keywords || "").toLowerCase().indexOf(lower) >= 0 ||
        (a.body_md || "").toLowerCase().indexOf(lower) >= 0;
    });
    shellHelpRenderList(filtered);
  };

  window.shellHelpOpenArticle = function(slug) {
    shellHelpCurrentSlug = slug;
    var article = shellHelpArticles.find(function(a) { return a.slug === slug; });
    if (!article) {
      // Try fetching by slug from API
      fetch(PLATFORM_URL + "/api/help/slug/" + encodeURIComponent(slug), { credentials: "include" })
        .then(function(r) { if (!r.ok) throw new Error("Not found"); return r.json(); })
        .then(function(a) { shellHelpRenderArticle(a); })
        .catch(function() {
          document.getElementById("platform-help-list").innerHTML = '<div style="color:#999;padding:20px;text-align:center;">Article not found</div>';
        });
      return;
    }
    shellHelpRenderArticle(article);
  };

  function shellHelpRenderArticle(article) {
    var listEl = document.getElementById("platform-help-list");
    var html = '<button class="shell-help-back-btn" onclick="event.stopPropagation();shellHelpBack()">← Back to list</button>';
    html += '<div class="help-article-view">';
    html += '<h1>' + article.title + '</h1>';
    html += shellHelpRenderMd(article.body_md || "");
    html += '</div>';
    listEl.innerHTML = html;
    document.getElementById("platform-help-search").style.display = "none";
  }

  window.shellHelpBack = function() {
    shellHelpCurrentSlug = null;
    document.getElementById("platform-help-search").style.display = "";
    shellHelpRenderList(shellHelpArticles);
  };

  function shellHelpRenderMd(md) {
    if (!md) return '<p style="color:#999">No content</p>';
    var html = md
      .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
      .replace(/^### (.+)$/gm, '<h3>$1</h3>')
      .replace(/^## (.+)$/gm, '<h2>$1</h2>')
      .replace(/^# (.+)$/gm, '<h1>$1</h1>')
      .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
      .replace(/(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)/g, '<em>$1</em>')
      .replace(/`([^`]+)`/g, '<code>$1</code>')
      .replace(/^[\-\*] (.+)$/gm, '<li>$1</li>')
      .replace(/\n\n/g, '</p><p>')
      .replace(/\n/g, '<br>');
    // Wrap consecutive <li> into <ul>
    html = html.replace(/(<li>.*?<\/li>(?:<br>)?)+/g, function(match) {
      return '<ul>' + match.replace(/<br>/g, '') + '</ul>';
    });
    return html;
  }

  // Global open-help function (for product deep-linking)
  window.shellOpenHelp = function(slug) {
    var panel = document.getElementById("platform-help-panel");
    panel.classList.add("open");
    helpBtn.classList.add("active");
    if (slug) {
      shellHelpOpenArticle(slug);
    } else {
      shellHelpLoadArticles();
    }
  };

  // AI button toggle
  var aiBtn = document.getElementById("platform-ai-btn");
  aiBtn.addEventListener("click", function() {
    var panel = document.getElementById("platform-ai-panel");
    var isOpen = panel.classList.toggle("open");
    aiBtn.classList.toggle("active", isOpen);
    if (isOpen) document.getElementById("platform-ai-input").focus();
  });

  // Close AI panel when clicking outside
  document.addEventListener("click", function(e) {
    var panel = document.getElementById("platform-ai-panel");
    if (!panel.classList.contains("open")) return;
    var notchAi = document.getElementById("platform-notch-ai-btn");
    if (panel.contains(e.target) || aiBtn.contains(e.target) || (notchAi && notchAi.contains(e.target))) return;
    panel.classList.remove("open");
    aiBtn.classList.remove("active");
  });

  // Simple echo-style AI send (placeholder for real LLM integration)
  window.shellAiSend = function() {
    var input = document.getElementById("platform-ai-input");
    var msg = input.value.trim();
    if (!msg) return;
    input.value = "";
    var container = document.getElementById("platform-ai-messages");
    container.innerHTML += '<div class="ai-msg user">' + msg.replace(/</g, "&lt;") + '</div>';
    container.innerHTML += '<div class="ai-msg assistant" style="opacity:0.6">Searching help articles...</div>';
    container.scrollTop = container.scrollHeight;

    // RAG: search help articles for relevant context
    var products = (user && user.products) ? user.products.join(",") : "";
    fetch(PLATFORM_URL + "/api/help?q=" + encodeURIComponent(msg) + "&products=" + encodeURIComponent(products), { credentials: "include" })
      .then(function(r) { return r.json(); })
      .then(function(articles) {
        var msgs = container.querySelectorAll(".ai-msg");
        var last = msgs[msgs.length - 1];
        last.style.opacity = "1";

        if (articles && articles.length > 0) {
          // Build response from matching articles
          var top = articles.slice(0, 3);
          var response = '<strong>I found ' + articles.length + ' relevant article' + (articles.length > 1 ? 's' : '') + ':</strong><br><br>';
          top.forEach(function(a) {
            response += '<div style="margin-bottom:8px;padding:6px 10px;background:#f8f9fa;border-radius:5px;cursor:pointer;" onclick="shellOpenHelp(\'' + a.slug + '\')">';
            response += '<strong>' + a.title + '</strong>';
            if (a.body_md) {
              var preview = a.body_md.replace(/[#*`\-]/g, '').substring(0, 120);
              response += '<br><span style="font-size:11px;color:#666;">' + preview + '...</span>';
            }
            response += '</div>';
          });
          if (articles.length > 3) {
            response += '<div style="font-size:11px;color:#888;margin-top:4px;">...and ' + (articles.length - 3) + ' more. Try refining your question.</div>';
          }
          last.innerHTML = response;
        } else {
          last.textContent = "I couldn't find any help articles matching your question. Try different keywords, or browse the help panel (? icon) for available topics.";
        }
        container.scrollTop = container.scrollHeight;
      })
      .catch(function() {
        var msgs = container.querySelectorAll(".ai-msg");
        var last = msgs[msgs.length - 1];
        last.style.opacity = "1";
        last.textContent = "Sorry, I couldn't search the help articles right now. Please try again later.";
        container.scrollTop = container.scrollHeight;
      });
  };

  // ── Try loading a custom logo image (uses bootstrap data if available, falls back to API) ──
  function applyLogoConfig(cfg) {
    if (!cfg || !cfg.use_custom_logo) return;
    var logoSpan = document.getElementById("platform-shell-logo");
    var img = new Image();
    img.onload = function() {
      logoSpan.textContent = "";
      img.alt = "Logo";
      logoSpan.appendChild(img);
    };
    img.src = PLATFORM_URL + "/shell-logo.png";
  }

  // ── Notch pill — shows inbox count when shell is unpinned ──
  const notchPill = document.createElement("div");
  notchPill.className = "shell-notch-pill";
  notchPill.id = "platform-notch-pill";
  notchPill.innerHTML = `
    <div class="notch-content">
      <button class="notch-inbox-btn" id="platform-notch-inbox-btn" title="Master Inbox — aggregated tasks from all products.">
        <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 11l3 3l8 -8"></path><path d="M20 12v6a2 2 0 0 1 -2 2h-12a2 2 0 0 1 -2 -2v-12a2 2 0 0 1 2 -2h9"></path></svg>
        Inbox <span class="notch-count" id="platform-notch-count">0</span>
      </button>
      <button class="notch-inbox-btn" id="platform-notch-help-btn" title="Help" style="gap:0;padding:3px 7px;">
        <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>
      </button>
      <button class="notch-inbox-btn" id="platform-notch-ai-btn" title="AI Assistant" style="gap:0;padding:3px 7px;">
        <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3l1.912 5.813a2 2 0 0 0 1.275 1.275L21 12l-5.813 1.912a2 2 0 0 0-1.275 1.275L12 21l-1.912-5.813a2 2 0 0 0-1.275-1.275L3 12l5.813-1.912a2 2 0 0 0 1.275-1.275L12 3z"/><path d="M19 2l.5 1.5L21 4l-1.5.5L19 6l-.5-1.5L17 4l1.5-.5L19 2z"/></svg>
      </button>
      <button class="notch-pin-btn" id="platform-notch-pin-btn" title="Pin the shell bar back to the top of the page.">
        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M15 4.5l-4 4l-4 1.5l-1.5 1.5l7 7l1.5 -1.5l1.5 -4l4 -4"/><line x1="9" y1="15" x2="4.5" y2="19.5"/><line x1="14.5" y1="4" x2="20" y2="9.5"/></svg>
      </button>
    </div>
    <div class="notch-tail"></div>
    <div class="shell-inbox-dropdown" id="platform-notch-dropdown">
      <div class="inbox-header">
        <span>Inbox</span>
        <span id="platform-notch-summary" style="font-size:11px;color:#999;font-weight:400"></span>
      </div>
      <div id="platform-notch-list">
        <div class="inbox-empty">No items</div>
      </div>
    </div>
  `;
  document.body.prepend(notchPill);

  // Notch inbox button → toggle dropdown
  var notchInboxBtn = document.getElementById("platform-notch-inbox-btn");
  notchInboxBtn.addEventListener("click", function(e) {
    e.stopPropagation();
    var dd = document.getElementById("platform-notch-dropdown");
    dd.classList.toggle("open");
  });
  // Notch pin button → re-pin the shell bar
  var notchPinBtn = document.getElementById("platform-notch-pin-btn");
  notchPinBtn.addEventListener("click", function(e) {
    e.stopPropagation();
    userHasToggledPin = true;
    applyPinState(true);
  });
  // Notch AI button → toggle AI panel
  var notchAiBtn = document.getElementById("platform-notch-ai-btn");
  notchAiBtn.addEventListener("click", function(e) {
    e.stopPropagation();
    var panel = document.getElementById("platform-ai-panel");
    var isOpen = panel.classList.toggle("open");
    aiBtn.classList.toggle("active", isOpen);
    if (isOpen) document.getElementById("platform-ai-input").focus();
  });
  // Notch Help button → toggle help panel
  var notchHelpBtn = document.getElementById("platform-notch-help-btn");
  notchHelpBtn.addEventListener("click", function(e) {
    e.stopPropagation();
    var panel = document.getElementById("platform-help-panel");
    var isOpen = panel.classList.toggle("open");
    helpBtn.classList.toggle("active", isOpen);
    if (isOpen) shellHelpLoadArticles();
  });
  document.addEventListener("click", function(e) {
    var dd = document.getElementById("platform-notch-dropdown");
    if (dd && !notchPill.contains(e.target)) dd.classList.remove("open");
    var extDd = document.getElementById("platform-ext-dropdown");
    if (extDd && !extDd.parentElement.contains(e.target)) extDd.classList.remove("open");
  });

  // ── Inject user info into sidebar ──
  function injectUserInfo() {
    var sidebar = document.querySelector(".sidebar-nav");
    if (!sidebar || document.getElementById("shell-user-injected")) return;
    var initials = user.name.split(" ").map(function(w) { return w[0]; }).join("").toUpperCase().slice(0, 2);
    var el = document.createElement("div");
    el.className = "shell-user-info";
    el.id = "shell-user-injected";
    el.innerHTML = '<div class="user-avatar">' + initials + '</div>' +
      '<div class="user-details">' +
        '<span class="user-name">' + user.name + '</span>' +
        '<span class="user-role">' + (user.role || 'User') + '</span>' +
      '</div>';
    sidebar.parentNode.insertBefore(el, sidebar);
  }
  // Inject on load and observe for sidebar open
  injectUserInfo();
  new MutationObserver(injectUserInfo).observe(document.body, { childList: true, subtree: true });

  // Populate navigation async
  // ── Pin / Unpin logic ──
  var pinBtn = document.getElementById("platform-pin-btn");
  var userHasToggledPin = false; // Track if user manually changed pin state

  function applyPinState(pinned) {
    if (pinned) {
      header.classList.remove("unpinned");
      header.classList.remove("peek");
      pinBtn.classList.add("pinned");
      hoverZone.classList.remove("active");
      document.body.style.paddingTop = "32px";
      document.body.style.setProperty("--shell-height", "32px");
      updateNotchVisibility();
    } else {
      header.classList.add("unpinned");
      pinBtn.classList.remove("pinned");
      hoverZone.classList.add("active");
      document.body.style.paddingTop = "0";
      document.body.style.setProperty("--shell-height", "0px");
      updateNotchVisibility();
    }
  }

  var _lastPendingCount = 0;
  function updateNotchVisibility() {
    var unpinned = header.classList.contains("unpinned");
    var peeking = header.classList.contains("peek");
    var hasTasks = _lastPendingCount > 0;
    var notchCount = document.getElementById("platform-notch-count");
    // Show notch when unpinned, but NOT while peeking (shell bar is visible)
    if (unpinned && !peeking) {
      notchPill.classList.add("visible");
      if (notchCount) notchCount.style.display = hasTasks ? "" : "none";
    } else {
      notchPill.classList.remove("visible");
      var dd = document.getElementById("platform-notch-dropdown");
      if (dd) dd.classList.remove("open");
    }
  }

  function isPinned() {
    return !header.classList.contains("unpinned");
  }

  if (pinBtn) {
    pinBtn.addEventListener("click", function() {
      var nowPinned = !isPinned();
      userHasToggledPin = true;
      applyPinState(nowPinned);
    });
  }

  // Hover zone → peek
  hoverZone.addEventListener("mouseenter", function() {
    if (!isPinned()) {
      header.classList.add("peek");
      notchPill.classList.remove("visible");
    }
  });
  header.addEventListener("mouseleave", function() {
    if (!isPinned()) {
      header.classList.remove("peek");
      updateNotchVisibility();
    }
  });
  header.addEventListener("mouseenter", function() {
    if (!isPinned()) {
      header.classList.add("peek");
      notchPill.classList.remove("visible");
    }
  });

  var lastProductCount = 0;

  function refreshShellNav() {
    // Prefer bootstrap data (already loaded), fall back to individual endpoint
    var navPromise = _bootstrapData && _bootstrapData.navigation
      ? Promise.resolve(_bootstrapData.navigation)
      : loadNavigation();
    navPromise.then(function(navData) {
      var navItems = navData.items;
      var extTools = navData.externalTools || [];
      var nav = document.getElementById("platform-shell-nav");
      if (!nav) return;

      var currentKey = detectCurrentProduct(navItems);
      var multiProduct = navItems.length > 1;

      if (multiProduct || extTools.length > 0) {
        var tooltips = {
          platform: 'Platform Admin — system configuration, economy domain, sync, dimension policies, external tools, and user management.',
          prod_a: 'Product A (Budget & Planning) — standalone container with its own backend and database. Receives shared dimensions and facts from the platform layer via Kafka. SSO via shared platform token.',
          prod_b: 'Product B (Analytics) — standalone container with its own backend and database. Receives shared dimensions and facts from the platform layer via Kafka. SSO via shared platform token.'
        };
        var html = navItems.map(function(item) {
          var href = resolveUrl(item);
          var isActive = item.key === currentKey;
          var tip = tooltips[item.key] || 'Navigate to ' + item.label + '. Same login session via shared platform token.';
          return '<a href="' + href + '" class="' + (isActive ? 'active' : '') + '" title="' + tip + '">' + item.label + '</a>';
        }).join("");

        // External tools
        if (extTools.length > 0) {
          // External tools (arrow icon distinguishes them)
          if (extTools.length <= 3) {
            // Direct links
            html += extTools.map(function(t) {
              return '<a href="' + t.url + '" target="_blank" rel="noopener" class="shell-ext-link" title="External tool: ' + t.label + '. Opens in a new tab. Configured per customer in Platform Admin → External Tools.">' + t.label + '<svg class="ext-icon" viewBox="0 0 12 12"><path d="M3.5 1.5H1.5v9h9v-2M6.5 1.5h4v4M10.5 1.5L5 7" fill="none" stroke="currentColor" stroke-width="1.2" stroke-linecap="round" stroke-linejoin="round"/></svg></a>';
            }).join("");
          } else {
            // First 2 direct, rest in dropdown
            html += extTools.slice(0, 2).map(function(t) {
              return '<a href="' + t.url + '" target="_blank" rel="noopener" class="shell-ext-link" title="External tool: ' + t.label + '. Opens in a new tab. Configured per customer in Platform Admin → External Tools.">' + t.label + '<svg class="ext-icon" viewBox="0 0 12 12"><path d="M3.5 1.5H1.5v9h9v-2M6.5 1.5h4v4M10.5 1.5L5 7" fill="none" stroke="currentColor" stroke-width="1.2" stroke-linecap="round" stroke-linejoin="round"/></svg></a>';
            }).join("");
            html += '<span style="position:relative;display:inline-flex;align-items:center">';
            html += '<span class="shell-ext-more" id="platform-ext-more-btn">Tools <svg viewBox="0 0 10 6" style="width:8px;height:6px;margin-left:2px;vertical-align:middle"><path d="M1 1l4 4 4-4" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></svg></span>';
            html += '<div class="shell-ext-dropdown" id="platform-ext-dropdown">';
            html += extTools.slice(2).map(function(t) {
              return '<a href="' + t.url + '" target="_blank" rel="noopener" title="External tool: ' + t.label + '. Opens in a new tab.">' + t.label + '<svg class="ext-icon" viewBox="0 0 12 12"><path d="M3.5 1.5H1.5v9h9v-2M6.5 1.5h4v4M10.5 1.5L5 7" fill="none" stroke="currentColor" stroke-width="1.2" stroke-linecap="round" stroke-linejoin="round"/></svg></a>';
            }).join("");
            html += '</div>';
            html += '</span>';
          }
        }

        nav.innerHTML = html;

        // Bind dropdown toggle if present
        var moreBtn = document.getElementById("platform-ext-more-btn");
        if (moreBtn) {
          moreBtn.addEventListener("click", function(e) {
            e.stopPropagation();
            var dd = document.getElementById("platform-ext-dropdown");
            if (dd) dd.classList.toggle("open");
          });
        }
      } else {
        nav.innerHTML = '<span style="color:#aaa;font-weight:500;font-size:11px">Single-product mode</span>';
      }

      // Update pin state when product count changes, or on initial load
      var totalCount = navItems.length + extTools.length;
      var countChanged = lastProductCount !== 0 && totalCount !== lastProductCount;
      if (!userHasToggledPin || countChanged) {
        applyPinState(multiProduct || extTools.length > 0);
        if (countChanged) userHasToggledPin = false;
      }
      lastProductCount = totalCount;
    });
  }

  // Initial load — single bootstrap request covers nav + config + help
  loadBootstrap().then(function(data) {
    if (data && data.config) applyLogoConfig(data.config);
    refreshShellNav();
  }).catch(function() {
    // Bootstrap failed — fall back to individual requests
    fetch(PLATFORM_URL + "/api/shell-config", { credentials: "include" })
      .then(function(r) { return r.json(); })
      .then(function(cfg) { applyLogoConfig(cfg); })
      .catch(function() {});
    refreshShellNav();
  });

  // Expose global reload so admin page (or any host) can trigger a nav refresh
  window.__platformShellReloadNav = refreshShellNav;

  // ── Inbox ──
  const inboxBtn = document.getElementById("platform-inbox-btn");
  const inboxDropdown = document.getElementById("platform-inbox-dropdown");
  const inboxCount = document.getElementById("platform-inbox-count");
  const inboxList = document.getElementById("platform-inbox-list");
  const inboxSummary = document.getElementById("platform-inbox-summary");

  if (inboxBtn) {
    inboxBtn.addEventListener("click", function(e) {
      e.stopPropagation();
      inboxDropdown.classList.toggle("open");
    });

    document.addEventListener("click", function(e) {
      if (!inboxDropdown.contains(e.target) && e.target !== inboxBtn) {
        inboxDropdown.classList.remove("open");
      }
    });

    async function loadInbox() {
      try {
        var items = await (await fetch(PLATFORM_URL + "/api/inbox", { credentials: "include" })).json();
        var pending = items.filter(function(i) { return i.status === "pending"; });
        var count = pending.length;

        // Update shell bar inbox button
        if (count > 0) {
          inboxCount.textContent = count;
          inboxCount.style.display = "";
        } else {
          inboxCount.style.display = "none";
        }

        inboxSummary.textContent = count + " active";

        // Update notch pill count + visibility
        _lastPendingCount = count;
        var notchCount = document.getElementById("platform-notch-count");
        if (notchCount) notchCount.textContent = count;
        updateNotchVisibility();

        // Build item HTML (shared between shell dropdown and notch dropdown)
        var itemsHtml;
        if (!items.length) {
          itemsHtml = '<div class="inbox-empty">No items</div>';
        } else {
          itemsHtml = items.map(function(item) {
            var isDone = item.status !== "pending";
            var link = item.resolved_link || item.link || "";
            var priorityDot = item.priority === "high" ? ' style="background:#ef4444"' : '';
            var sourceLabels = { prod_a: "Budget", prod_b: "Analytics", erp: "ERP", platform: "Platform" };
            var sourceLabel = sourceLabels[item.source] || item.source;
            return '<div class="shell-inbox-item ' + (isDone ? "done" : "") + '" data-id="' + item.id + '" data-link="' + link + '">' +
              '<div class="inbox-dot"' + priorityDot + '></div>' +
              '<div class="inbox-text">' +
                '<div class="inbox-title">' + item.title + '</div>' +
                (item.description ? '<div class="inbox-desc">' + item.description + '</div>' : '') +
              '</div>' +
              '<button class="inbox-complete-btn" data-complete-id="' + item.id + '" title="Mark as done">&#x2714;</button>' +
              '<span class="inbox-source">' + sourceLabel + '</span>' +
            '</div>';
          }).join("");
        }

        // Populate both lists
        inboxList.innerHTML = itemsHtml;
        var notchList = document.getElementById("platform-notch-list");
        var notchSummary = document.getElementById("platform-notch-summary");
        if (notchList) notchList.innerHTML = itemsHtml;
        if (notchSummary) notchSummary.textContent = count + " active";

        // Wire up buttons for both lists
        [inboxList, notchList].forEach(function(list) {
          if (!list) return;
          list.querySelectorAll(".inbox-complete-btn").forEach(function(btn) {
            btn.addEventListener("click", async function(e) {
              e.stopPropagation();
              var id = btn.getAttribute("data-complete-id");
              await fetch(PLATFORM_URL + "/api/inbox/" + id, {
                method: "PATCH",
                headers: { "Content-Type": "application/json" },
                credentials: "include",
                body: JSON.stringify({ status: "done" })
              });
              loadInbox();
            });
          });
          list.querySelectorAll(".shell-inbox-item").forEach(function(el) {
            el.addEventListener("click", function() {
              var link = el.getAttribute("data-link");
              if (link) window.location.href = link;
            });
          });
        });
      } catch(e) { /* ignore */ }
    }

    loadInbox();
    setInterval(loadInbox, 5000);

    // Allow product iframes to trigger immediate inbox refresh
    window.addEventListener("message", function(e) {
      if (e.data && e.data.type === "refresh-inbox") loadInbox();
    });
  }

  // ── End of error isolation ──
  } catch(shellErr) {
    console.error("[Platform Shell v" + (typeof SHELL_VERSION !== "undefined" ? SHELL_VERSION : "?") + "] Error — host product unaffected:", shellErr);
  }
})();
