(function () {
    if (window.__siteBroadcastInit) return;
    window.__siteBroadcastInit = true;

    const path = (location.pathname || "/").toLowerCase();
    const pageKey = path === "/" || path.startsWith("/home") ? "home"
        : path.startsWith("/index") ? "index"
        : path.startsWith("/database") ? "database"
        : path.startsWith("/collector-monitor") ? "monitor"
        : path.startsWith("/api-status") ? "api_status"
        : path.startsWith("/changelog") ? "changelog"
        : "all";

    const cidKey = "gn_client_id";
    let clientId = "";
    try {
        clientId = localStorage.getItem(cidKey) || "";
        if (!clientId) {
            const rnd = (window.crypto && typeof window.crypto.randomUUID === "function")
                ? window.crypto.randomUUID()
                : `cid-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
            clientId = rnd;
            localStorage.setItem(cidKey, clientId);
        }
    } catch (_) {
        clientId = `cid-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
    }

    let pollTimer = null;
    let presenceTimer = null;
    let inFlight = false;
    let activeToast = null;
    let activeBroadcast = null;

    const wrap = document.createElement("div");
    wrap.className = "admin-toast-wrap";
    Object.assign(wrap.style, {
        position: "absolute",
        left: "14px",
        right: "auto",
        bottom: "auto",
        zIndex: "3200",
        pointerEvents: "none",
    });
    document.body.appendChild(wrap);

    function placeWrapNearViewportBottom() {
        const top = Math.max(8, window.scrollY + window.innerHeight - 110);
        wrap.style.top = `${top}px`;
    }
    let scrollTicking = false;
    function onScrollOrResize() {
        if (scrollTicking) return;
        scrollTicking = true;
        requestAnimationFrame(() => {
            placeWrapNearViewportBottom();
            scrollTicking = false;
        });
    }
    window.addEventListener("scroll", onScrollOrResize, { passive: true });
    window.addEventListener("resize", onScrollOrResize);
    placeWrapNearViewportBottom();

    function mapToastType(type) {
        const rawType = String(type || "info").toLowerCase();
        if (rawType === "success" || rawType === "ok") return "ok";
        if (rawType === "warn" || rawType === "warning") return "warn";
        if (rawType === "error" || rawType === "bad") return "error";
        return "info";
    }

    function ensurePersistentToast(message, type) {
        placeWrapNearViewportBottom();
        const mapped = mapToastType(type);
        if (!activeToast) {
            activeToast = document.createElement("div");
            activeToast.className = "admin-toast";
            wrap.appendChild(activeToast);
            requestAnimationFrame(() => activeToast && activeToast.classList.add("show"));
        }
        activeToast.dataset.type = mapped;
        activeToast.innerText = message || "";
    }

    function clearPersistentToast() {
        if (!activeToast) return;
        const node = activeToast;
        activeToast = null;
        node.classList.remove("show");
        setTimeout(() => node.remove(), 240);
    }

    function upsertEmergencyBanner(banner) {
        let el = document.getElementById("devEmergencyBanner");
        if (!banner || !banner.text) {
            if (el) el.remove();
            return;
        }
        if (!el) {
            el = document.createElement("div");
            el.id = "devEmergencyBanner";
            el.className = "dev-emergency-banner";
            document.body.appendChild(el);
        }
        el.dataset.type = String(banner.type || "warn").toLowerCase();
        el.innerText = banner.text;
    }

    function applyFeatureFlags(flags) {
        const f = flags || {};
        document.body.classList.toggle("dev-flag-no-anim", !!f.disable_animations);
        document.body.classList.toggle("dev-flag-hide-stars", !!f.hide_star_badges);
        window.__DEV_PAUSE_AUTO_REFRESH = !!f.pause_auto_refresh;
    }

    async function sendPresence() {
        try {
            await fetch("/api/client_presence", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    client_id: clientId,
                    page: pageKey,
                    broadcast_id: activeBroadcast?.id || "",
                    variant: activeBroadcast?.variant || "",
                }),
            });
        } catch (_) {}
    }

    async function poll() {
        if (inFlight) return;
        inFlight = true;
        try {
            const q = `t=${Date.now()}&page=${encodeURIComponent(pageKey)}&client_id=${encodeURIComponent(clientId)}`;
            const r = await fetch(`/api/live_status?${q}`, { cache: "no-store" });
            if (!r.ok) return;
            const d = await r.json();
            activeBroadcast = d.broadcast || null;
            if (activeBroadcast && activeBroadcast.message) {
                ensurePersistentToast(activeBroadcast.message, activeBroadcast.type);
            } else {
                clearPersistentToast();
            }
            applyFeatureFlags(d.feature_flags || {});
            upsertEmergencyBanner(d.emergency_banner || null);
            sendPresence();
        } catch (_) {
        } finally {
            inFlight = false;
        }
    }

    function start() {
        if (!pollTimer) {
            placeWrapNearViewportBottom();
            poll();
            pollTimer = setInterval(() => { if (!document.hidden) poll(); }, 2000);
        }
        if (!presenceTimer) {
            presenceTimer = setInterval(() => { if (!document.hidden) sendPresence(); }, 12000);
        }
    }

    function stop() {
        if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
        if (presenceTimer) { clearInterval(presenceTimer); presenceTimer = null; }
    }

    document.addEventListener("visibilitychange", () => {
        if (document.hidden) stop();
        else start();
    });

    start();
})();
