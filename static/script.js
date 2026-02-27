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

function installAvatarFallback(img) {
    if (!img) return;
    img.onerror = () => {
        if (img.dataset.fallbackApplied === "1") return;
        img.dataset.fallbackApplied = "1";
        img.src = AVATAR_PLACEHOLDER;
    };
}

const SETTINGS_DEFAULTS = {
    theme: "light",
    refreshIntervalMs: 30000,
    compactCards: false,
    reducedMotion: false,
    showStars: true,
    showTerminated: true,
};

function markAvatarLoaded(img) {
    if (!img) return;
    img.classList.remove("loading-avatar");
    const shell = img.closest(".avatar-shell");
    if (shell) shell.classList.add("is-ready");
}

function updatePageLoadUI(stageText, loaded, total) {
    const panel = document.getElementById("pageLoadStatus");
    const textEl = document.getElementById("pageLoadText");
    const progressEl = document.getElementById("pageLoadProgress");
    const barEl = document.getElementById("pageLoadBar");

    if (!panel || !textEl || !progressEl || !barEl) return;

    const safeTotal = total > 0 ? total : 1;
    const pct = Math.max(0, Math.min(100, Math.round((loaded / safeTotal) * 100)));

    panel.style.display = "block";
    textEl.innerText = stageText;
    progressEl.innerText = `${loaded}/${total}`;
    barEl.style.width = `${pct}%`;
}

// ---------------- Persistent Dark Mode ----------------
const darkModeBtn = document.getElementById("darkModeBtn");
const lightModeBtn = document.getElementById("lightModeBtn");
const refreshIntervalSelect = document.getElementById("refreshIntervalSelect");
const toggleCompactCards = document.getElementById("toggleCompactCards");
const toggleReducedMotion = document.getElementById("toggleReducedMotion");
const toggleShowStars = document.getElementById("toggleShowStars");
const toggleShowTerminated = document.getElementById("toggleShowTerminated");
const resetSettingsBtn = document.getElementById("resetSettingsBtn");
const body = document.body;

function loadSettings() {
    const refreshStored = parseInt(localStorage.getItem("refresh_interval_ms"), 10);
    return {
        theme: localStorage.getItem("theme") || SETTINGS_DEFAULTS.theme,
        refreshIntervalMs: Number.isFinite(refreshStored) ? refreshStored : SETTINGS_DEFAULTS.refreshIntervalMs,
        compactCards: localStorage.getItem("compact_cards") === "1",
        reducedMotion: localStorage.getItem("reduced_motion") === "1",
        showStars: localStorage.getItem("show_stars") !== "0",
        showTerminated: localStorage.getItem("show_terminated") !== "0",
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
}

function saveSettings(settings) {
    localStorage.setItem("theme", settings.theme);
    localStorage.setItem("refresh_interval_ms", String(settings.refreshIntervalMs));
    localStorage.setItem("compact_cards", settings.compactCards ? "1" : "0");
    localStorage.setItem("reduced_motion", settings.reducedMotion ? "1" : "0");
    localStorage.setItem("show_stars", settings.showStars ? "1" : "0");
    localStorage.setItem("show_terminated", settings.showTerminated ? "1" : "0");
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

searchSelect.forEach(btn => {
    if (btn.dataset.type === "settings") {
        btn.addEventListener("click", () => {
            settingsModal.classList.toggle("hidden");
        });
    }
});

closeSettings?.addEventListener("click", () => settingsModal.classList.add("hidden"));
cornerSettingsBtn?.addEventListener("click", () => settingsModal?.classList.toggle("hidden"));

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
                };

                if (card && payload.is_terminated) {
                    showTerminatedBadge(uid);
                }
                if (card && payload.is_star_creator) showStarBadge(uid);

                if (card && payload.is_bought) {
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
                    img.addEventListener("load", async () => {
                        await finalizeOne(img);
                        done();
                    }, { once: true });
                    img.addEventListener("error", async () => {
                        await finalizeOne(img);
                        done();
                    }, { once: true });
                    img.src = payload.avatar_url || AVATAR_PLACEHOLDER;
                });
            }
            if (runId !== batchLoadRunId) return;
            updatePageLoadUI("Users ready", total, total);
        })
        .catch(() => {
            document.querySelectorAll(".avatar-shell").forEach(shell => shell.classList.add("is-ready"));
            document.querySelectorAll(".avatar-small").forEach(img => img.classList.remove("loading-avatar"));
            updatePageLoadUI("Loading unavailable", 0, total);
        });
}

// ---------------- Fetch full user data only on click ----------------
function fetchUserModal(uid, callback) {
    if (userDataCache[uid]?.fullData) return callback(userDataCache[uid].fullData);

    if (currentControllers[uid]) currentControllers[uid].abort();
    const controller = new AbortController();
    currentControllers[uid] = controller;

    fetch(`/user/${uid}`, { signal: controller.signal })
        .then(r => r.json())
        .then(data => {
            userDataCache[uid].fullData = data;
            callback(data);
        }).catch(err => {
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

    card.addEventListener("click", () => {
        modal.classList.add("show");
        modalContent.classList.add("is-loading");
        header.innerHTML = ""; pSource.innerHTML = ""; extra.innerHTML = "";
        profileBtn.style.display = "none";

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
                extra.innerHTML = "Error loading user";
                return;
            }

            const sourceHTML = `
                ${data.stored.source === "Seed List" ? `<span class="badge badge-seed">${data.stored.source}</span>` : `<span class="badge badge-new">Newly Added</span>`}
                ${data.is_star_creator ? '<span class="badge badge-star shiny">Star Creator</span>' : ''}
                ${data.stored?.bought_tag ? '<span class="badge badge-bought">Bought Check</span>' : ''}
            `;

            header.innerHTML = `${data.stored.username} (${uid})`;
            pSource.innerHTML = sourceHTML;

            installAvatarFallback(avatarLarge);
            avatarLarge.src = data.avatar_url || AVATAR_PLACEHOLDER;
            avatarLarge.style.display = "block";

            extra.innerHTML = `
                <div class="profile-grid">
                    <div><span class="k">Display</span><span class="v">${data.live?.displayName || "N/A"}</span></div>
                    <div><span class="k">Joined</span><span class="v">${data.live?.joined || "N/A"}</span></div>
                    <div><span class="k">Friends</span><span class="v">${data.stats.friends}</span></div>
                    <div><span class="k">Followers</span><span class="v">${data.stats.followers}</span></div>
                    <div><span class="k">Following</span><span class="v">${data.stats.following}</span></div>
                    <div><span class="k">User ID</span><span class="v">${uid}</span></div>
                </div>
            `;

            profileBtn.href = data.profile_url;
            profileBtn.style.display = "inline-block";
            requestAnimationFrame(() => modalContent.classList.remove("is-loading"));
        });
    });

    closeBtn.addEventListener("click", () => {
        modal.classList.remove("show");
        modalContent.classList.remove("is-loading");
    });
}

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

    closeBtn.addEventListener("click", () => modal.classList.remove("show"));
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
            window.location.reload();
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
    liveStatusPollHandle = setInterval(pollLiveStatus, appSettings.refreshIntervalMs);
}

startLiveStatusPolling();
