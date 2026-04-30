// Platform Shell — Injected header for all products
// Loaded via: <script src="http://localhost:3000/shell.js"></script>

(function () {
  const PLATFORM_URL = "http://localhost:3000";

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

  // Fetch dynamic navigation from platform API
  async function loadNavigation() {
    try {
      const res = await fetch(PLATFORM_URL + "/api/navigation", { credentials: "include" });
      if (!res.ok) return { items: [], externalTools: [] };
      var data = await res.json();
      // Support both old array format and new object format
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
      <span class="logo">Platform POC</span>
      <nav id="platform-shell-nav"><span style="color:#aaa;font-weight:500;font-size:11px">Loading...</span></nav>
      <div class="spacer"></div>
      <div style="position:relative">
        <button class="shell-inbox-btn" id="platform-inbox-btn">
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
      <button class="shell-pin-btn" id="platform-pin-btn" title="Pin/unpin navigation bar"><svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M15 4.5l-4 4l-4 1.5l-1.5 1.5l7 7l1.5 -1.5l1.5 -4l4 -4"/><line x1="9" y1="15" x2="4.5" y2="19.5"/><line x1="14.5" y1="4" x2="20" y2="9.5"/></svg></button>
    </div>
  `;
  document.body.prepend(header);

  // ── Notch pill — shows inbox count when shell is unpinned ──
  const notchPill = document.createElement("div");
  notchPill.className = "shell-notch-pill";
  notchPill.id = "platform-notch-pill";
  notchPill.innerHTML = `
    <div class="notch-content">
      <button class="notch-inbox-btn" id="platform-notch-inbox-btn">
        <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 11l3 3l8 -8"></path><path d="M20 12v6a2 2 0 0 1 -2 2h-12a2 2 0 0 1 -2 -2v-12a2 2 0 0 1 2 -2h9"></path></svg>
        Inbox <span class="notch-count" id="platform-notch-count">0</span>
      </button>
      <button class="notch-pin-btn" id="platform-notch-pin-btn" title="Pin navigation bar">
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
    loadNavigation().then(function(navData) {
      var navItems = navData.items;
      var extTools = navData.externalTools || [];
      var nav = document.getElementById("platform-shell-nav");
      if (!nav) return;

      var currentKey = detectCurrentProduct(navItems);
      var multiProduct = navItems.length > 1;

      if (multiProduct || extTools.length > 0) {
        var html = navItems.map(function(item) {
          var href = resolveUrl(item);
          var isActive = item.key === currentKey;
          return '<a href="' + href + '" class="' + (isActive ? 'active' : '') + '">' + item.label + '</a>';
        }).join("");

        // External tools
        if (extTools.length > 0) {
          // External tools (arrow icon distinguishes them)
          if (extTools.length <= 3) {
            // Direct links
            html += extTools.map(function(t) {
              return '<a href="' + t.url + '" target="_blank" rel="noopener" class="shell-ext-link">' + t.label + '<svg class="ext-icon" viewBox="0 0 12 12"><path d="M3.5 1.5H1.5v9h9v-2M6.5 1.5h4v4M10.5 1.5L5 7" fill="none" stroke="currentColor" stroke-width="1.2" stroke-linecap="round" stroke-linejoin="round"/></svg></a>';
            }).join("");
          } else {
            // First 2 direct, rest in dropdown
            html += extTools.slice(0, 2).map(function(t) {
              return '<a href="' + t.url + '" target="_blank" rel="noopener" class="shell-ext-link">' + t.label + '<svg class="ext-icon" viewBox="0 0 12 12"><path d="M3.5 1.5H1.5v9h9v-2M6.5 1.5h4v4M10.5 1.5L5 7" fill="none" stroke="currentColor" stroke-width="1.2" stroke-linecap="round" stroke-linejoin="round"/></svg></a>';
            }).join("");
            html += '<span style="position:relative;display:inline-flex;align-items:center">';
            html += '<span class="shell-ext-more" id="platform-ext-more-btn">Tools <svg viewBox="0 0 10 6" style="width:8px;height:6px;margin-left:2px;vertical-align:middle"><path d="M1 1l4 4 4-4" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></svg></span>';
            html += '<div class="shell-ext-dropdown" id="platform-ext-dropdown">';
            html += extTools.slice(2).map(function(t) {
              return '<a href="' + t.url + '" target="_blank" rel="noopener">' + t.label + '<svg class="ext-icon" viewBox="0 0 12 12"><path d="M3.5 1.5H1.5v9h9v-2M6.5 1.5h4v4M10.5 1.5L5 7" fill="none" stroke="currentColor" stroke-width="1.2" stroke-linecap="round" stroke-linejoin="round"/></svg></a>';
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

  // Initial load
  refreshShellNav();

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
})();
