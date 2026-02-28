// ---------------- Elements ----------------
const searchSelect = document.querySelectorAll(".tab-btn");
const resultsSection = document.querySelector(".results");
const searchSection = document.querySelector(".search-section");
const searchInput = document.getElementById("searchInput");
const userDataCache = {};
let currentControllers = {};
let lastDbMtime = null;
let liveStatusPollHandle = null;
let batchLoadRunId = 0;
const AVATAR_PLACEHOLDER = "data:image/svg+xml;utf8,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 120 120'%3E%3Cdefs%3E%3ClinearGradient id='g' x1='0' x2='1' y1='0' y2='1'%3E%3Cstop offset='0' stop-color='%230f172a'/%3E%3Cstop offset='1' stop-color='%231e3a8a'/%3E%3C/linearGradient%3E%3C/defs%3E%3Crect width='120' height='120' rx='60' fill='url(%23g)'/%3E%3Ccircle cx='60' cy='46' r='22' fill='%2393c5fd' fill-opacity='0.9'/%3E%3Cpath d='M24 103c6-17 20-28 36-28s30 11 36 28' fill='%2393c5fd' fill-opacity='0.9'/%3E%3C/svg%3E";
const evidenceOverlay = document.getElementById("evidenceOverlay");
const evidenceCloseBtn = document.getElementById("evidenceCloseBtn");
const evidenceTitle = document.getElementById("evidenceTitle");
const evidenceBody = document.getElementById("evidenceBody");
const quickHelpToggle = document.getElementById("quickHelpToggle");
const quickHelpPanel = document.getElementById("quickHelpPanel");
let pageLoadHideTimer = null;
let breachOverlayEl = null;
let breachAudioCtx = null;
let breachAudioEl = null;
let breachPlaying = false;
let breachHideTimer = null;
let pendingAutoReload = false;

function isUiBusyForReload() {
    const activeModal = document.querySelector(".modal.show");
    const evidenceOpen = !!(evidenceOverlay && !evidenceOverlay.classList.contains("hidden"));
    const breachOpen = !!(breachOverlayEl && !breachOverlayEl.classList.contains("hidden"));
    const settingsOpen = !!(settingsModal && !settingsModal.classList.contains("hidden"));
    return !!(activeModal || evidenceOpen || breachPlaying || breachOpen || settingsOpen);
}

function tryRunDeferredReload() {
    if (!pendingAutoReload) return;
    if (isUiBusyForReload()) return;
    pendingAutoReload = false;
    window.location.reload();
}

function ensureBreachOverlay() {
    if (breachOverlayEl) {
        breachOverlayEl.remove();
        breachOverlayEl = null;
    }
    const el = document.createElement("div");
    el.id = "breachModeOverlay";
    el.className = "breach-mode-overlay hidden";
    el.innerHTML = `
        <div class="breach-bg"></div>
        <div class="breach-marquee top">TOS BREACHER • BOUGHT CHECKMARK • POLICY FLAG • TOS BREACHER • BOUGHT CHECKMARK • POLICY FLAG •</div>
        <div class="breach-marquee bottom">OOOH • MLG ALERT • OOOH • MLG ALERT • OOOH • MLG ALERT •</div>
        <div class="breach-chaos" aria-hidden="true">
            <img class="mlg-img doge i1" src="/static/mlg/doge.png" alt="Doge" onerror="this.style.display='none'">
            <img class="mlg-img doge i2" src="/static/mlg/doge.png" alt="" onerror="this.style.display='none'">
            <img class="mlg-img chip i3" src="/static/mlg/dorito.png" alt="Dorito chip" onerror="this.style.display='none'">
            <img class="mlg-img chip i4" src="/static/mlg/dorito.png" alt="" onerror="this.style.display='none'">
            <img class="mlg-img dew i5" src="/static/mlg/mtn_dew.png" alt="Mountain Dew" onerror="this.style.display='none'">
            <img class="mlg-img dew i6" src="/static/mlg/mtn_dew.png" alt="" onerror="this.style.display='none'">
            <img class="mlg-img glasses i7" src="/static/mlg/glasses.png" alt="Deal with it glasses" onerror="this.style.display='none'">
            <img class="mlg-img glasses i8" src="/static/mlg/glasses.png" alt="" onerror="this.style.display='none'">
            <img class="mlg-img wasted i9" src="/static/mlg/wasted.png" alt="Wasted text" onerror="this.style.display='none'">
            <img class="mlg-img illuminati i10" src="/static/mlg/illuminati.png" alt="Illuminati" onerror="this.style.display='none'">
            <img class="mlg-img illuminati i11" src="/static/mlg/illuminati.png" alt="" onerror="this.style.display='none'">
            <img class="mlg-img chip i12" src="/static/mlg/dorito.png" alt="" onerror="this.style.display='none'">
            <span class="sticker s1">MLG</span>
            <span class="sticker s2">OOOH</span>
            <span class="sticker s3">TOS BREACH</span>
            <span class="sticker s4">NO WAY</span>
            <span class="sticker s5">CHECKMARK BUYER</span>
            <span class="sticker s6">POLICY FLAG</span>
            <span class="sticker s7">SYSTEM ALERT</span>
            <span class="sticker s8">REPORTED VIBES</span>
            <span class="sticker s9">EXPOSED</span>
            <span class="sticker s10">BROKE TOS</span>
            <span class="sticker s11">OHHHH</span>
            <span class="sticker s12">MOD WATCHING</span>
            <span class="sticker s13">FAKE VERIFIED</span>
            <span class="sticker s14">BIG YIKES</span>
            <span class="sticker s15">POLICY HIT</span>
            <span class="sticker s16">BOUGHT BADGE</span>
        </div>
        <div class="breach-lines">
            <div class="breach-line main">YOU... HAVE CLICKED ON A FAKER.</div>
            <div class="breach-line sub">TOS BREACHER DETECTED</div>
            <div class="breach-line spin">BOUGHT CHECKMARK ALERT</div>
            <div class="breach-timer" id="breachTimer">10</div>
        </div>
    `;
    document.body.appendChild(el);
    // Hard fallback styling so text/images always render even if CSS gets overridden.
    Object.assign(el.style, {
        position: "fixed",
        inset: "0",
        zIndex: "99999",
        visibility: "visible",
        opacity: "1",
    });
    const lines = el.querySelector(".breach-lines");
    if (lines) Object.assign(lines.style, { position: "fixed", inset: "0", zIndex: "100001", pointerEvents: "none" });
    const forceLine = (selector, top, size, color) => {
        const n = el.querySelector(selector);
        if (!n) return;
        Object.assign(n.style, {
            position: "fixed",
            left: "50%",
            top,
            transform: "translateX(-50%)",
            fontSize: size,
            fontWeight: "900",
            textTransform: "uppercase",
            color,
            zIndex: "100002",
            display: "block",
            opacity: "1",
            visibility: "visible",
            whiteSpace: "nowrap",
        });
    };
    forceLine(".breach-line.main", "12vh", "clamp(1.9rem, 6.2vw, 4.8rem)", "#ffffff");
    forceLine(".breach-line.sub", "31vh", "clamp(1.4rem, 4.4vw, 3rem)", "#fecaca");
    forceLine(".breach-line.spin", "52vh", "clamp(1.2rem, 3.4vw, 2.2rem)", "#fde68a");
    forceLine(".breach-timer", "72vh", "clamp(2.4rem, 7vw, 5rem)", "#f8fafc");
    const marqueeTop = el.querySelector(".breach-marquee.top");
    const marqueeBottom = el.querySelector(".breach-marquee.bottom");
    if (marqueeTop) Object.assign(marqueeTop.style, {
        position: "fixed", top: "2vh", left: "0", right: "0", zIndex: "100003",
        display: "block", opacity: "1", visibility: "visible", textAlign: "center", color: "#fff"
    });
    if (marqueeBottom) Object.assign(marqueeBottom.style, {
        position: "fixed", bottom: "2vh", left: "0", right: "0", zIndex: "100003",
        display: "block", opacity: "1", visibility: "visible", textAlign: "center", color: "#fff"
    });

    const imgPos = {
        "i1": { left: "1%", bottom: "2%", width: "20vw", maxWidth: "320px" },
        "i2": { left: "70%", top: "56%", width: "14vw", maxWidth: "220px" },
        "i3": { left: "8%", top: "44%", width: "12vw", maxWidth: "190px" },
        "i4": { right: "2%", top: "54%", width: "10vw", maxWidth: "170px" },
        "i5": { left: "2%", top: "9%", width: "14vw", maxWidth: "220px" },
        "i6": { left: "44%", bottom: "10%", width: "12vw", maxWidth: "190px" },
        "i7": { right: "10%", top: "7%", width: "24vw", maxWidth: "390px" },
        "i8": { right: "36%", top: "24%", width: "16vw", maxWidth: "250px" },
        "i9": { left: "30%", top: "2%", width: "30vw", maxWidth: "520px" },
        "i10": { left: "3%", top: "65%", width: "12vw", maxWidth: "190px" },
        "i11": { right: "43%", top: "66%", width: "10vw", maxWidth: "170px" },
        "i12": { left: "83%", top: "41%", width: "10vw", maxWidth: "170px" },
    };
    el.querySelectorAll(".mlg-img").forEach((img) => {
        const posClass = Array.from(img.classList).find(c => /^i\d+$/.test(c));
        const p = posClass ? imgPos[posClass] : null;
        Object.assign(img.style, {
            position: "fixed",
            display: "block",
            visibility: "visible",
            opacity: "1",
            zIndex: "100000",
            pointerEvents: "none",
        });
        if (p) Object.assign(img.style, p);
    });
    el.style.visibility = "hidden";
    el.style.opacity = "0";
    el.style.display = "none";
    breachOverlayEl = el;
    return el;
}

function playBreachAudio(durationMs = 10000) {
    // Prefer user-supplied MP3 first.
    try {
        if (!breachAudioEl) {
            breachAudioEl = new Audio("/static/audio/swaggityswagger.mp3");
            breachAudioEl.preload = "auto";
        }
        breachAudioEl.currentTime = 0;
        breachAudioEl.volume = 0.95;
        breachAudioEl.play().catch(() => {});
        setTimeout(() => {
            try {
                breachAudioEl.pause();
                breachAudioEl.currentTime = 0;
            } catch (_) {}
        }, durationMs);
        return;
    } catch (_) {}

    // Fallback synth if browser blocks/doesn't load media.
    try {
        const AudioContextCtor = window.AudioContext || window.webkitAudioContext;
        if (!AudioContextCtor) return;
        breachAudioCtx = new AudioContextCtor();
        const ctx = breachAudioCtx;
        ctx.resume?.().catch(() => {});
        const now = ctx.currentTime + 0.02;
        const endAt = now + durationMs / 1000;

        const note = (freq, start, len, type = "sawtooth", vol = 0.08) => {
            const osc = ctx.createOscillator();
            const gain = ctx.createGain();
            const lp = ctx.createBiquadFilter();
            lp.type = "lowpass";
            lp.frequency.setValueAtTime(type === "triangle" ? 900 : 1800, start);
            osc.type = type;
            osc.frequency.setValueAtTime(freq, start);
            gain.gain.setValueAtTime(0.0001, start);
            gain.gain.exponentialRampToValueAtTime(vol, start + 0.015);
            gain.gain.exponentialRampToValueAtTime(0.0001, start + len);
            osc.connect(lp).connect(gain).connect(ctx.destination);
            osc.start(start);
            osc.stop(start + len + 0.03);
        };
        const ooh = (start, base = 330, len = 0.48, vol = 0.14) => {
            const osc1 = ctx.createOscillator();
            const osc2 = ctx.createOscillator();
            const gain = ctx.createGain();
            const lp = ctx.createBiquadFilter();
            lp.type = "lowpass";
            lp.frequency.setValueAtTime(1100, start);
            lp.frequency.exponentialRampToValueAtTime(700, start + len);

            osc1.type = "triangle";
            osc2.type = "sine";
            osc1.frequency.setValueAtTime(base * 1.02, start);
            osc2.frequency.setValueAtTime(base * 0.99, start);
            osc1.frequency.exponentialRampToValueAtTime(base * 0.72, start + len);
            osc2.frequency.exponentialRampToValueAtTime(base * 0.7, start + len);

            gain.gain.setValueAtTime(0.0001, start);
            gain.gain.exponentialRampToValueAtTime(vol, start + 0.045);
            gain.gain.exponentialRampToValueAtTime(0.0001, start + len);

            osc1.connect(lp);
            osc2.connect(lp);
            lp.connect(gain).connect(ctx.destination);
            osc1.start(start);
            osc2.start(start);
            osc1.stop(start + len + 0.05);
            osc2.stop(start + len + 0.05);
        };

        const kick = (start) => {
            const osc = ctx.createOscillator();
            const gain = ctx.createGain();
            osc.type = "sine";
            osc.frequency.setValueAtTime(160, start);
            osc.frequency.exponentialRampToValueAtTime(45, start + 0.14);
            gain.gain.setValueAtTime(0.22, start);
            gain.gain.exponentialRampToValueAtTime(0.0001, start + 0.16);
            osc.connect(gain).connect(ctx.destination);
            osc.start(start);
            osc.stop(start + 0.17);
        };

        const chordProg = [
            [220, 277.18, 329.63],
            [246.94, 311.13, 369.99],
            [196, 246.94, 293.66],
            [174.61, 220, 261.63],
        ];
        const leadSeq = [659.25, 783.99, 880, 987.77, 880, 783.99, 1046.5, 1174.66];

        const barLen = 0.96;
        let bar = 0;
        while (now + bar * barLen < endAt) {
            const barStart = now + bar * barLen;
            const chord = chordProg[bar % chordProg.length];
            chord.forEach((f) => note(f, barStart, barLen * 0.88, "triangle", 0.055));
            note(chord[0] / 2, barStart, barLen * 0.82, "sine", 0.1);
            kick(barStart);
            kick(barStart + 0.48);

            for (let i = 0; i < 8; i++) {
                const st = barStart + i * 0.12;
                if (st >= endAt) break;
                note(leadSeq[(bar * 2 + i) % leadSeq.length], st, 0.1, "sawtooth", 0.05);
            }
            const chantStart = barStart + 0.18;
            if (chantStart < endAt) ooh(chantStart, 370, 0.42, 0.13);
            if (chantStart + 0.46 < endAt) ooh(chantStart + 0.46, 415, 0.4, 0.12);
            bar += 1;
        }

        // Final stinger
        if (endAt - now > 0.8) {
            note(523.25, endAt - 0.42, 0.36, "sawtooth", 0.12);
            note(659.25, endAt - 0.38, 0.34, "triangle", 0.11);
            note(783.99, endAt - 0.34, 0.3, "triangle", 0.1);
        }

        setTimeout(() => {
            ctx.close().catch(() => {});
            breachAudioCtx = null;
        }, durationMs + 300);
    } catch (_) {
        breachAudioCtx = null;
    }
}

function runBreachMode(durationMs = 10000) {
    if (breachPlaying) return Promise.resolve();
    breachPlaying = true;
    const overlay = ensureBreachOverlay();
    const timerEl = overlay.querySelector("#breachTimer");
    overlay.classList.remove("hidden");
    overlay.style.display = "grid";
    overlay.style.visibility = "visible";
    overlay.style.opacity = "1";
    document.body.classList.add("breach-rainbow-mode");
    playBreachAudio(durationMs);

    let remaining = Math.ceil(durationMs / 1000);
    if (timerEl) timerEl.innerText = String(remaining);
    const interval = setInterval(() => {
        remaining -= 1;
        if (timerEl) timerEl.innerText = String(Math.max(0, remaining));
    }, 1000);

    const stopBreachMode = () => {
        clearInterval(interval);
        if (overlay) {
            overlay.classList.add("hidden");
            overlay.style.display = "none";
            overlay.style.visibility = "hidden";
            overlay.style.opacity = "0";
        }
        document.body.classList.remove("breach-rainbow-mode");
        try {
            if (breachAudioEl) {
                breachAudioEl.pause();
                breachAudioEl.currentTime = 0;
            }
        } catch (_) {}
        breachPlaying = false;
    };

    return new Promise(resolve => {
        breachHideTimer = setTimeout(() => {
            stopBreachMode();
            resolve();
        }, durationMs);
    });
}

function installAvatarFallback(img) {
    if (!img) return;
    img.onerror = () => {
        if (img.dataset.fallbackApplied === "1") return;
        img.dataset.fallbackApplied = "1";
        img.src = AVATAR_PLACEHOLDER;
    };
}

function safeStorageGet(key) {
    try {
        return localStorage.getItem(key);
    } catch (_) {
        return null;
    }
}

function safeStorageSet(key, value) {
    try {
        localStorage.setItem(key, value);
    } catch (_) {
        // Ignore storage failures (private mode / blocked storage).
    }
}

const SETTINGS_DEFAULTS = {
    theme: "light",
    refreshIntervalMs: 30000,
    compactCards: false,
    reducedMotion: false,
    showStars: true,
    showTerminated: true,
    mlgEffects: false,
};

function markAvatarLoaded(img) {
    if (!img) return;
    img.classList.remove("loading-avatar");
    const shell = img.closest(".avatar-shell");
    if (shell) shell.classList.add("is-ready");
}

function closeEvidenceOverlay() {
    if (!evidenceOverlay) return;
    evidenceOverlay.classList.add("hidden");
}

async function openEvidenceOverlay(uid, username) {
    if (!evidenceOverlay || !evidenceBody || !evidenceTitle) return;
    evidenceOverlay.classList.remove("hidden");
    evidenceTitle.innerText = `Evidence - ${username} (${uid})`;
    evidenceBody.innerHTML = '<div class="evidence-empty">Loading evidence...</div>';
    try {
        const res = await fetch(`/api/evidence/${uid}`, { cache: "no-store" });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Failed to load evidence");
        const items = Array.isArray(data.items) ? data.items : [];
        if (!items.length) {
            evidenceBody.innerHTML = '<div class="evidence-empty">No evidence has been attached for this profile yet.</div>';
            return;
        }
        evidenceBody.innerHTML = "";
        items.forEach((item, idx) => {
            const box = document.createElement("div");
            box.className = "evidence-item";
            const when = item.updated_ts ? new Date(item.updated_ts * 1000).toLocaleString() : "Unknown";
            const safeTitle = item.title || `Evidence ${idx + 1}`;
            const urlPart = item.url
                ? `<div><a href="${item.url}" target="_blank" rel="noopener noreferrer">${item.url}</a></div>`
                : "";
            const notePart = item.note ? `<div class="note">${item.note}</div>` : "";
            box.innerHTML = `
                <div class="head"><span>${item.source_type || "other"}</span><span>${when}</span></div>
                <div class="title">${safeTitle}</div>
                ${urlPart}
                ${notePart}
            `;
            evidenceBody.appendChild(box);
        });
    } catch (err) {
        evidenceBody.innerHTML = `<div class="evidence-empty">${err?.message || "Unable to load evidence."}</div>`;
    }
}

function updatePageLoadUI(stageText, loaded, total) {
    const panel = document.getElementById("pageLoadStatus");
    const textEl = document.getElementById("pageLoadText");
    const progressEl = document.getElementById("pageLoadProgress");
    const barEl = document.getElementById("pageLoadBar");

    if (!panel || !textEl || !progressEl || !barEl) return;

    const safeTotal = total > 0 ? total : 1;
    const pct = Math.max(0, Math.min(100, Math.round((loaded / safeTotal) * 100)));

    panel.classList.remove("hidden");
    panel.style.display = "block";
    textEl.innerText = stageText;
    progressEl.innerText = `${loaded}/${total}`;
    barEl.style.width = `${pct}%`;

    if (pageLoadHideTimer) clearTimeout(pageLoadHideTimer);
    const isReady = (total > 0 && loaded >= total) || /users ready/i.test(stageText);
    const isUnavailable = /unavailable/i.test(stageText);
    if (isReady) {
        pageLoadHideTimer = setTimeout(() => {
            panel.classList.add("hidden");
            panel.style.display = "";
        }, 900);
    } else if (isUnavailable) {
        pageLoadHideTimer = setTimeout(() => {
            panel.classList.add("hidden");
            panel.style.display = "";
        }, 1600);
    }
}

// ---------------- Persistent Dark Mode ----------------
const darkModeBtn = document.getElementById("darkModeBtn");
const lightModeBtn = document.getElementById("lightModeBtn");
const refreshIntervalSelect = document.getElementById("refreshIntervalSelect");
const toggleCompactCards = document.getElementById("toggleCompactCards");
const toggleReducedMotion = document.getElementById("toggleReducedMotion");
const toggleShowStars = document.getElementById("toggleShowStars");
const toggleShowTerminated = document.getElementById("toggleShowTerminated");
const toggleMlgEffects = document.getElementById("toggleMlgEffects");
const resetSettingsBtn = document.getElementById("resetSettingsBtn");
const body = document.body;

function loadSettings() {
    const refreshStored = parseInt(safeStorageGet("refresh_interval_ms"), 10);
    return {
        theme: safeStorageGet("theme") || SETTINGS_DEFAULTS.theme,
        refreshIntervalMs: Number.isFinite(refreshStored) ? refreshStored : SETTINGS_DEFAULTS.refreshIntervalMs,
        compactCards: safeStorageGet("compact_cards") === "1",
        reducedMotion: safeStorageGet("reduced_motion") === "1",
        showStars: safeStorageGet("show_stars") !== "0",
        showTerminated: safeStorageGet("show_terminated") !== "0",
        mlgEffects: safeStorageGet("mlg_effects") === "1",
    };
}

function applySettings(settings) {
    body.classList.toggle("dark-mode", settings.theme === "dark");
    body.classList.toggle("compact-cards", !!settings.compactCards);
    body.classList.toggle("reduced-motion", !!settings.reducedMotion);
    body.classList.toggle("hide-stars", !settings.showStars);
    body.classList.toggle("hide-terminated", !settings.showTerminated);

    if (refreshIntervalSelect) refreshIntervalSelect.value = String(settings.refreshIntervalMs);
    if (toggleCompactCards) toggleCompactCards.checked = !!settings.compactCards;
    if (toggleReducedMotion) toggleReducedMotion.checked = !!settings.reducedMotion;
    if (toggleShowStars) toggleShowStars.checked = !!settings.showStars;
    if (toggleShowTerminated) toggleShowTerminated.checked = !!settings.showTerminated;
    if (toggleMlgEffects) toggleMlgEffects.checked = !!settings.mlgEffects;
}

function saveSettings(settings) {
    safeStorageSet("theme", settings.theme);
    safeStorageSet("refresh_interval_ms", String(settings.refreshIntervalMs));
    safeStorageSet("compact_cards", settings.compactCards ? "1" : "0");
    safeStorageSet("reduced_motion", settings.reducedMotion ? "1" : "0");
    safeStorageSet("show_stars", settings.showStars ? "1" : "0");
    safeStorageSet("show_terminated", settings.showTerminated ? "1" : "0");
    safeStorageSet("mlg_effects", settings.mlgEffects ? "1" : "0");
}

let appSettings = loadSettings();
applySettings(appSettings);

darkModeBtn?.addEventListener("click", () => {
    appSettings.theme = "dark";
    applySettings(appSettings);
    saveSettings(appSettings);
});
lightModeBtn?.addEventListener("click", () => {
    appSettings.theme = "light";
    applySettings(appSettings);
    saveSettings(appSettings);
});
refreshIntervalSelect?.addEventListener("change", () => {
    const v = parseInt(refreshIntervalSelect.value, 10);
    appSettings.refreshIntervalMs = Number.isFinite(v) ? v : SETTINGS_DEFAULTS.refreshIntervalMs;
    applySettings(appSettings);
    saveSettings(appSettings);
    startLiveStatusPolling();
});
toggleCompactCards?.addEventListener("change", () => {
    appSettings.compactCards = !!toggleCompactCards.checked;
    applySettings(appSettings);
    saveSettings(appSettings);
});
toggleReducedMotion?.addEventListener("change", () => {
    appSettings.reducedMotion = !!toggleReducedMotion.checked;
    applySettings(appSettings);
    saveSettings(appSettings);
});
toggleShowStars?.addEventListener("change", () => {
    appSettings.showStars = !!toggleShowStars.checked;
    applySettings(appSettings);
    saveSettings(appSettings);
});
toggleShowTerminated?.addEventListener("change", () => {
    appSettings.showTerminated = !!toggleShowTerminated.checked;
    applySettings(appSettings);
    saveSettings(appSettings);
});
toggleMlgEffects?.addEventListener("change", () => {
    appSettings.mlgEffects = !!toggleMlgEffects.checked;
    applySettings(appSettings);
    saveSettings(appSettings);
});
resetSettingsBtn?.addEventListener("click", () => {
    appSettings = { ...SETTINGS_DEFAULTS };
    applySettings(appSettings);
    saveSettings(appSettings);
    startLiveStatusPolling();
});

// ---------------- Settings Modal ----------------
const settingsModal = document.getElementById("settings-modal");
const closeSettings = document.getElementById("closeSettings");
const cornerSettingsBtn = document.getElementById("cornerSettingsBtn");
let settingsOpenTimer = null;
let settingsCloseTimer = null;

function openSettingsModal() {
    if (!settingsModal) return;
    if (settingsCloseTimer) {
        clearTimeout(settingsCloseTimer);
        settingsCloseTimer = null;
    }
    settingsModal.classList.remove("hidden");
    if (settingsOpenTimer) clearTimeout(settingsOpenTimer);
    settingsOpenTimer = setTimeout(() => {
        settingsModal.classList.add("is-open");
    }, 10);
}

function closeSettingsModal() {
    if (!settingsModal) return;
    if (settingsOpenTimer) {
        clearTimeout(settingsOpenTimer);
        settingsOpenTimer = null;
    }
    settingsModal.classList.remove("is-open");
    if (settingsCloseTimer) clearTimeout(settingsCloseTimer);
    settingsCloseTimer = setTimeout(() => {
        settingsModal.classList.add("hidden");
    }, 220);
}

function toggleSettingsModal() {
    if (!settingsModal) return;
    if (settingsModal.classList.contains("hidden")) {
        openSettingsModal();
    } else {
        closeSettingsModal();
    }
}

searchSelect.forEach(btn => {
    if (btn.dataset.type === "settings") {
        btn.addEventListener("click", () => {
            toggleSettingsModal();
        });
    }
});

closeSettings?.addEventListener("click", closeSettingsModal);
cornerSettingsBtn?.addEventListener("click", toggleSettingsModal);

if (quickHelpPanel) {
    // Convert initial hidden state into animated collapsed state.
    if (quickHelpPanel.classList.contains("hidden")) {
        quickHelpPanel.classList.remove("hidden");
        quickHelpPanel.classList.add("quick-help-collapsed");
    }
    if (quickHelpToggle) quickHelpToggle.setAttribute("aria-expanded", "false");
}

quickHelpToggle?.addEventListener("click", () => {
    if (!quickHelpPanel) return;
    quickHelpPanel.classList.toggle("quick-help-collapsed");
    const expanded = !quickHelpPanel.classList.contains("quick-help-collapsed");
    quickHelpToggle.setAttribute("aria-expanded", expanded ? "true" : "false");
});

// ---------------- Draggable Settings ----------------
if (settingsModal) {
    let isDragging = false, offsetX = 0, offsetY = 0;
    const header = settingsModal.querySelector(".settings-header");

    header.addEventListener("mousedown", (e) => {
        isDragging = true;
        offsetX = e.clientX - settingsModal.offsetLeft;
        offsetY = e.clientY - settingsModal.offsetTop;
    });

    document.addEventListener("mousemove", (e) => {
        if (isDragging) {
            settingsModal.style.left = (e.clientX - offsetX) + "px";
            settingsModal.style.top = (e.clientY - offsetY) + "px";
        }
    });

    document.addEventListener("mouseup", () => { isDragging = false; });
}

// ---------------- Tabs Logic ----------------
searchSelect.forEach(btn => {
    btn.addEventListener("click", () => {
        const type = btn.dataset.type;
        if (type === "settings") return;

        const url = new URL(window.location.href);

        // Set search_type explicitly to the tab clicked
        url.searchParams.set("search_type", type);

        // Reset page
        url.searchParams.set("page", 1);

        // Clear query only when switching away from individual
        if (type !== "individual") {
            url.searchParams.delete("query");
        }

        window.location.href = url.toString();
    });
});

// ---------------- Batch fetch avatars ----------------
function batchFetchAvatars() {
    const runId = ++batchLoadRunId;
    const uids = Array.from(document.querySelectorAll(".user-card")).map(c => c.dataset.uid);
    if (uids.length === 0) return;
    const total = uids.length;
    updatePageLoadUI("Loading avatars...", 0, total);

    const showStarBadge = (uid) => {
        const card = document.querySelector(`.user-card[data-uid="${uid}"]`);
        const starBadge = card?.querySelector(".badge-star");
        if (starBadge) starBadge.style.display = "inline-block";
        if (userDataCache[uid]) userDataCache[uid].is_star_creator = true;
    };
    const showTerminatedBadge = (uid) => {
        const card = document.querySelector(`.user-card[data-uid="${uid}"]`);
        const ribbon = card?.querySelector(".banned-ribbon");
        if (ribbon) ribbon.style.display = "block";
        if (userDataCache[uid]) userDataCache[uid].is_terminated = true;
    };
    const fetchStarsFor = (targetUids) => {
        if (!targetUids.length) return;
        fetch(`/stars_batch?uids=${targetUids.join(",")}`)
            .then(r => r.json())
            .then(stars => {
                if (runId !== batchLoadRunId) return;
                for (const uid in stars) {
                    if (stars[uid]) showStarBadge(uid);
                }
            })
            .catch(() => {});
    };
    const fetchTerminatedFor = (targetUids, forceRefresh = false) => {
        if (!targetUids.length) return;
        fetch(`/terminated_batch?uids=${targetUids.join(",")}&force=${forceRefresh ? "1" : "0"}`)
            .then(r => r.json())
            .then(terminated => {
                if (runId !== batchLoadRunId) return;
                for (const uid in terminated) {
                    if (terminated[uid]) showTerminatedBadge(uid);
                }
            })
            .catch(() => {});
    };

    // Start badge checks immediately, in parallel with users_batch.
    fetchStarsFor(uids);
    fetchTerminatedFor(uids, false);

    fetch(`/users_batch?uids=${uids.join(",")}`)
        .then(r => r.json())
        .then(async data => {
            if (runId !== batchLoadRunId) return;
            updatePageLoadUI("Loading avatars...", 0, total);
            let processed = 0;
            const ordered = uids.map(uid => [uid, data[uid] || {}]);

            // Retry passes to avoid needing a manual refresh after transient API misses.
            setTimeout(() => {
                if (runId !== batchLoadRunId) return;
                const unresolvedStars = uids.filter(uid => !(userDataCache[uid] && userDataCache[uid].is_star_creator));
                const unresolvedTerminated = uids.filter(uid => !(userDataCache[uid] && userDataCache[uid].is_terminated));
                fetchStarsFor(unresolvedStars);
                fetchTerminatedFor(unresolvedTerminated, true);
            }, 500);
            setTimeout(() => {
                if (runId !== batchLoadRunId) return;
                const unresolvedStars = uids.filter(uid => !(userDataCache[uid] && userDataCache[uid].is_star_creator));
                const unresolvedTerminated = uids.filter(uid => !(userDataCache[uid] && userDataCache[uid].is_terminated));
                fetchStarsFor(unresolvedStars);
                fetchTerminatedFor(unresolvedTerminated, true);
            }, 1400);

            for (const [uid, payload] of ordered) {
                if (runId !== batchLoadRunId) return;

                const card = document.querySelector(`.user-card[data-uid="${uid}"]`);
                userDataCache[uid] = {
                    avatar_url: payload.avatar_url,
                    is_star_creator: !!payload.is_star_creator,
                    is_terminated: payload.is_terminated,
                    is_bought: !!payload.is_bought,
                    evidence_count: Number(payload.evidence_count || 0),
                    has_evidence: !!payload.has_evidence,
                };

                if (card && payload.is_terminated) {
                    showTerminatedBadge(uid);
                }
                if (card && payload.is_star_creator) showStarBadge(uid);

                if (card && payload.is_bought) {
                    card.dataset.bought = "1";
                    let boughtBadge = card.querySelector(".badge-bought");
                    if (!boughtBadge) {
                        boughtBadge = document.createElement("span");
                        boughtBadge.className = "badge badge-bought";
                        boughtBadge.innerText = "Bought Check";
                        const source = card.querySelector(".source");
                        if (source) source.appendChild(boughtBadge);
                    }
                }

                const finalizeOne = (img) =>
                    new Promise(done => {
                        if (runId !== batchLoadRunId) return done();
                        markAvatarLoaded(img);
                        processed += 1;
                        updatePageLoadUI("Loading users...", processed, total);
                        done();
                    });

                if (!card) {
                    await finalizeOne(null);
                    continue;
                }
                const img = card.querySelector(".avatar-small");
                if (!img) {
                    await finalizeOne(null);
                    continue;
                }

                await new Promise(done => {
                    installAvatarFallback(img);
                    let settled = false;
                    const finish = async () => {
                        if (settled) return;
                        settled = true;
                        clearTimeout(timeoutId);
                        await finalizeOne(img);
                        done();
                    };
                    img.addEventListener("load", async () => {
                        await finish();
                    }, { once: true });
                    img.addEventListener("error", async () => {
                        img.src = AVATAR_PLACEHOLDER;
                        await finish();
                    }, { once: true });
                    const timeoutId = setTimeout(async () => {
                        img.src = AVATAR_PLACEHOLDER;
                        await finish();
                    }, 4500);
                    img.src = payload.avatar_url || AVATAR_PLACEHOLDER;
                });
            }
            if (runId !== batchLoadRunId) return;
            updatePageLoadUI("Users ready", total, total);
        })
        .catch(() => {
            document.querySelectorAll(".avatar-shell").forEach(shell => shell.classList.add("is-ready"));
            document.querySelectorAll(".avatar-small").forEach(img => {
                installAvatarFallback(img);
                if (!img.getAttribute("src")) {
                    img.src = AVATAR_PLACEHOLDER;
                }
                img.classList.remove("loading-avatar");
            });
            updatePageLoadUI("Loading unavailable", 0, total);
        });
}

// ---------------- Fetch full user data only on click ----------------
function fetchUserModal(uid, callback) {
    if (userDataCache[uid]?.fullData && !userDataCache[uid]?.fullData?._partial) {
        return callback(userDataCache[uid].fullData);
    }

    if (currentControllers[uid]) currentControllers[uid].abort();
    const controller = new AbortController();
    currentControllers[uid] = controller;

    const requestUser = (withBust) => fetch(`/user/${uid}${withBust ? `?t=${Date.now()}` : ""}`, { signal: controller.signal })
        .then(r => r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`)));

    requestUser(false)
        .then(data => {
            if (!data?._partial) {
                if (!userDataCache[uid]) userDataCache[uid] = {};
                userDataCache[uid].fullData = data;
                return callback(data);
            }
            return requestUser(true).then(data2 => {
                if (!userDataCache[uid]) userDataCache[uid] = {};
                userDataCache[uid].fullData = data2;
                callback(data2);
            });
        })
        .catch(err => {
            if (err.name === "AbortError") return;
            callback(null);
        });
}

// ---------------- Setup card modal ----------------
function setupCardModal(card, uid) {
    const modal = document.getElementById(`modal-${uid}`);
    const modalContent = modal.querySelector(".modal-content");
    const closeBtn = modal.querySelector(".close");
    const avatarLarge = modal.querySelector(".avatar-large");
    const loadingIndicator = modal.querySelector(".loading-indicator");
    const loadingText = modal.querySelector(".loading-text");
    const header = modal.querySelector("h3");
    const pSource = modal.querySelector("p");
    const extra = modal.querySelector(".extra-info");
    const profileBtn = modal.querySelector(".profile-btn");
    const evidenceBtn = modal.querySelector(".evidence-btn");

    const showMlgDisabledNotice = () => {
        let notice = modal.querySelector(".mlg-warning-under-modal");
        if (!notice) {
            notice = document.createElement("div");
            notice.className = "mlg-warning-under-modal";
            notice.innerHTML = `
                <strong>MLG effects are OFF.</strong>
                This Bought Check profile has optional flashing effects. Open <strong>Settings</strong> to enable or keep disabled.
            `;
            if (modalContent?.parentNode === modal) {
                modal.insertBefore(notice, modalContent.nextSibling);
            } else {
                modal.appendChild(notice);
            }
        }
        notice.classList.add("show");
        if (showMlgDisabledNotice._timer) clearTimeout(showMlgDisabledNotice._timer);
        showMlgDisabledNotice._timer = setTimeout(() => notice.classList.remove("show"), 5200);
    };

    const openCardModalFlow = () => {
        modal.classList.add("show");
        modalContent.classList.add("is-loading");
        header.innerHTML = ""; pSource.innerHTML = ""; extra.innerHTML = "";
        profileBtn.style.display = "none";
        if (evidenceBtn) evidenceBtn.style.display = "none";

        if (loadingIndicator) loadingIndicator.style.display = "flex";
        loadingText.style.display = "block";
        avatarLarge.style.display = "none";
        avatarLarge.removeAttribute("src");

        fetchUserModal(uid, data => {
            if (loadingIndicator) loadingIndicator.style.display = "none";
            loadingText.style.display = "none";

            if (!data) {
                modalContent.classList.remove("is-loading");
                profileBtn.style.display = "none";
                if (evidenceBtn) evidenceBtn.style.display = "none";
                extra.innerHTML = "Error loading user";
                return;
            }

            const sourceHTML = `
                ${data.stored.source === "Seed List" ? `<span class="badge badge-seed">${data.stored.source}</span>` : `<span class="badge badge-new">Newly Added</span>`}
                ${data.stored?.manual_add ? '<span class="badge badge-manual">Manual Add</span>' : ''}
                ${data.is_star_creator ? '<span class="badge badge-star shiny">Star Creator</span>' : ''}
                ${data.stored?.bought_tag ? '<span class="badge badge-bought">Bought Check</span>' : ''}
                ${data.stored?.bought_tag && Number(userDataCache[uid]?.evidence_count || 0) > 0 ? `<span class="badge badge-new">Evidence ${Number(userDataCache[uid]?.evidence_count || 0)}</span>` : ''}
            `;

            header.innerHTML = `${data.stored.username} (${uid})`;
            pSource.innerHTML = sourceHTML;

            installAvatarFallback(avatarLarge);
            avatarLarge.src = data.avatar_url || AVATAR_PLACEHOLDER;
            avatarLarge.style.display = "block";

            extra.innerHTML = `
                ${data._partial ? '<div class="mlg-modal-note">Live profile stats are still syncing. Showing best available data.</div>' : ""}
                <div class="profile-grid">
                    <div><span class="k">Display</span><span class="v">${data.live?.displayName || "N/A"}</span></div>
                    <div><span class="k">Joined</span><span class="v">${data.live?.joined || "N/A"}</span></div>
                    <div><span class="k">Friends</span><span class="v">${data._partial ? "N/A" : data.stats.friends}</span></div>
                    <div><span class="k">Followers</span><span class="v">${data._partial ? "N/A" : data.stats.followers}</span></div>
                    <div><span class="k">Following</span><span class="v">${data._partial ? "N/A" : data.stats.following}</span></div>
                    <div><span class="k">User ID</span><span class="v">${uid}</span></div>
                </div>
            `;

            profileBtn.href = data.profile_url;
            profileBtn.style.display = "inline-block";
            if (evidenceBtn) {
                if (data.stored?.bought_tag) {
                    evidenceBtn.style.display = "inline-block";
                    evidenceBtn.onclick = () => openEvidenceOverlay(uid, data.stored?.username || uid);
                } else {
                    evidenceBtn.style.display = "none";
                    evidenceBtn.onclick = null;
                }
            }
            requestAnimationFrame(() => modalContent.classList.remove("is-loading"));
        });
    };

    card.addEventListener("click", async () => {
        const knownBought = !!userDataCache[uid]?.is_bought;
        const datasetBought = card.dataset.bought === "1";
        const domBought = !!card.querySelector(".badge-bought");
        if (knownBought || datasetBought || domBought) {
            if (appSettings.mlgEffects) {
                await runBreachMode(6000);
                // Hard cleanup in case any overlapping click/timer leaves residue.
                document.body.classList.remove("breach-rainbow-mode");
                const ov = document.getElementById("breachModeOverlay");
                if (ov) {
                    ov.classList.add("hidden");
                    ov.style.display = "none";
                    ov.style.visibility = "hidden";
                    ov.style.opacity = "0";
                }
                if (breachHideTimer) {
                    clearTimeout(breachHideTimer);
                    breachHideTimer = null;
                }
                try {
                    if (breachAudioEl) {
                        breachAudioEl.pause();
                        breachAudioEl.currentTime = 0;
                    }
                } catch (_) {}
                breachPlaying = false;
            } else {
                openCardModalFlow();
                setTimeout(showMlgDisabledNotice, 120);
                return;
            }
        }
        openCardModalFlow();
    });

    closeBtn.addEventListener("click", () => {
        modal.classList.remove("show");
        modalContent.classList.remove("is-loading");
        const notice = modal.querySelector(".mlg-warning-under-modal");
        if (notice) notice.classList.remove("show");
        setTimeout(tryRunDeferredReload, 60);
    });
}

evidenceCloseBtn?.addEventListener("click", closeEvidenceOverlay);
evidenceOverlay?.addEventListener("click", (e) => {
    if (e.target === evidenceOverlay) {
        closeEvidenceOverlay();
        setTimeout(tryRunDeferredReload, 60);
    }
});
document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") {
        closeEvidenceOverlay();
        document.querySelectorAll(".modal.show").forEach(m => m.classList.remove("show"));
        setTimeout(tryRunDeferredReload, 60);
    }
});

// ---------------- Setup all cards ----------------
document.querySelectorAll(".user-card").forEach(c => setupCardModal(c, c.dataset.uid));

// ---------------- Search on "Go" ----------------
const searchForm = document.getElementById("searchForm");
if (searchForm) {
    searchForm.addEventListener("submit", e => {
        e.preventDefault();
        const query = searchInput.value.trim();
        const url = new URL(window.location.href);
        url.searchParams.set("search_type", "individual");
        url.searchParams.set("query", query);
        url.searchParams.set("page", 1);
        window.location.href = url.toString();
    });
}

// ---------------- Jump to Page ----------------
const jumpBtn = document.getElementById("jumpPageBtn");
const jumpInput = document.getElementById("jumpPage");

jumpBtn?.addEventListener("click", () => {
    const target = parseInt(jumpInput.value);
    const max = parseInt(jumpInput.max);
    if (target >= 1 && target <= max) {
        const url = new URL(window.location.href);
        url.searchParams.set("page", target);
        window.location.href = url.toString();
    } else {
        alert(`Enter a valid page between 1 and ${max}`);
    }
});

// ---------------- Home Page Developer Card ----------------
const DEV_UID = "10006170169";

document.addEventListener("DOMContentLoaded", () => {
    const devCard = document.getElementById("devCard");
    if (!devCard) return;

    const modal = document.getElementById(`modal-${DEV_UID}`);
    const closeBtn = modal.querySelector(".close");
    const avatarLarge = modal.querySelector(".avatar-large");
    const loadingIndicator = modal.querySelector(".loading-indicator");
    const loadingText = modal.querySelector(".loading-text");
    const header = modal.querySelector("h3");
    const pSource = modal.querySelector("p");
    const extra = modal.querySelector(".extra-info");
    const profileBtn = modal.querySelector(".profile-btn");

    // Initially hide avatar and show loading
    avatarLarge.style.display = "none";
    if (loadingIndicator) loadingIndicator.style.display = "flex";
    loadingText.style.display = "block";

    // Fetch full user data from your backend (like other users)
    fetch(`/user/${DEV_UID}`)
        .then(res => res.json())
        .then(data => {
            // Set card info dynamically
            devCard.querySelector("h3").innerText = data.stored.username || "Developer";
            const devSmall = devCard.querySelector("img.avatar-small");
            installAvatarFallback(devSmall);
            devSmall.src = data.avatar_url || AVATAR_PLACEHOLDER;

            // Hide loading in card once avatar loaded
            if (loadingIndicator) loadingIndicator.style.display = "none";
            loadingText.style.display = "none";

            // Click event for modal
            devCard.addEventListener("click", () => {
                modal.classList.add("show");

                header.innerText = data.stored.username;

                // Badges
                let badges = `<span class="badge badge-dev">Developer</span>`;
                if (data.is_star_creator) {
                    badges += ' <span class="badge badge-star shiny">Star Creator</span>';
                }
                pSource.innerHTML = badges;

                installAvatarFallback(avatarLarge);
                avatarLarge.src = data.avatar_url || AVATAR_PLACEHOLDER;
                avatarLarge.style.display = "block";

                extra.innerHTML = `
                    <strong>Display Name:</strong> ${data.live?.displayName || "N/A"}<br>
                    <strong>Joined:</strong> ${data.live?.joined || "N/A"}<br>
                    <strong>Friends:</strong> ${data.stats.friends}<br>
                    <strong>Followers:</strong> ${data.stats.followers}<br>
                    <strong>Following:</strong> ${data.stats.following}
                `;

                profileBtn.href = data.profile_url;
            });
        })
        .catch(err => {
            console.error("Error fetching developer:", err);
            devCard.querySelector("h3").innerText = "Developer";
            if (loadingIndicator) loadingIndicator.style.display = "none";
            loadingText.style.display = "none";
        });

    closeBtn.addEventListener("click", () => {
        modal.classList.remove("show");
        setTimeout(tryRunDeferredReload, 60);
    });
});

// ---------------- Initial batch avatar fetch ----------------
document.querySelectorAll(".avatar-small").forEach(img => {
    if (img.complete && img.naturalWidth > 0) {
        markAvatarLoaded(img);
    }
});

batchFetchAvatars();

// ---------------- Live Sync Status + Auto Refresh ----------------
async function pollLiveStatus() {
    try {
        const res = await fetch(`/api/live_status?t=${Date.now()}`, { cache: "no-store" });
        if (!res.ok) return;
        const status = await res.json();

        const modeEl = document.getElementById("liveMode");
        const updatedEl = document.getElementById("liveUpdated");
        const totalsEl = document.getElementById("liveTotals");

        if (modeEl) modeEl.innerText = status.database_mode;
        if (updatedEl) updatedEl.innerText = `Last DB update: ${status.db_updated_at}`;
        if (totalsEl) {
            totalsEl.innerText = `Total: ${status.total_users} | Seed: ${status.seed_users} | Newly Added: ${status.new_users}`;
        }

        if (lastDbMtime !== null && status.db_mtime !== lastDbMtime) {
            if (isUiBusyForReload()) {
                pendingAutoReload = true;
            } else {
                window.location.reload();
            }
            return;
        }
        lastDbMtime = status.db_mtime;
    } catch (_) {
        // Keep UI functional even if status polling fails.
    }
}

function startLiveStatusPolling() {
    if (liveStatusPollHandle) clearInterval(liveStatusPollHandle);
    pollLiveStatus();
    liveStatusPollHandle = setInterval(() => {
        pollLiveStatus();
        tryRunDeferredReload();
    }, appSettings.refreshIntervalMs);
}

startLiveStatusPolling();
