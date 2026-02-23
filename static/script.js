// ---------------- Elements ----------------
const searchSelect = document.querySelectorAll(".tab-btn");
const resultsSection = document.querySelector(".results");
const searchSection = document.querySelector(".search-section");
const searchInput = document.getElementById("searchInput");
const userDataCache = {};
let currentControllers = {};

// ---------------- Persistent Dark Mode ----------------
const darkModeBtn = document.getElementById("darkModeBtn");
const lightModeBtn = document.getElementById("lightModeBtn");
const body = document.body;

if (localStorage.getItem("theme") === "dark") {
    body.classList.add("dark-mode");
}

darkModeBtn?.addEventListener("click", () => {
    body.classList.add("dark-mode");
    localStorage.setItem("theme", "dark");
});
lightModeBtn?.addEventListener("click", () => {
    body.classList.remove("dark-mode");
    localStorage.setItem("theme", "light");
});

// ---------------- Settings Modal ----------------
const settingsModal = document.getElementById("settings-modal");
const closeSettings = document.getElementById("closeSettings");

searchSelect.forEach(btn => {
    if (btn.dataset.type === "settings") {
        btn.addEventListener("click", () => {
            settingsModal.classList.toggle("hidden");
        });
    }
});

closeSettings?.addEventListener("click", () => settingsModal.classList.add("hidden"));

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
    const uids = Array.from(document.querySelectorAll(".user-card")).map(c => c.dataset.uid);
    if (uids.length === 0) return;

    fetch(`/users_batch?uids=${uids.join(",")}`)
        .then(r => r.json())
        .then(data => {
            for (const uid in data) {
                const card = document.querySelector(`.user-card[data-uid="${uid}"]`);
                if (card && data[uid].avatar_url) card.querySelector(".avatar-small").src = data[uid].avatar_url;

                if (data[uid].is_star_creator) {
                    const starBadge = card.querySelector(".badge-star");
                    if (starBadge) starBadge.style.display = "inline-block";
                }

                userDataCache[uid] = { avatar_url: data[uid].avatar_url, is_star_creator: data[uid].is_star_creator };
            }
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
    const closeBtn = modal.querySelector(".close");
    const avatarLarge = modal.querySelector(".avatar-large");
    const loadingText = modal.querySelector(".loading-text");
    const header = modal.querySelector("h3");
    const pSource = modal.querySelector("p");
    const extra = modal.querySelector(".extra-info");
    const profileBtn = modal.querySelector(".profile-btn");

    card.addEventListener("click", () => {
        modal.classList.add("show");
        header.innerHTML = ""; pSource.innerHTML = ""; extra.innerHTML = "";

        loadingText.style.display = "block";
        avatarLarge.style.display = "none";

        if (userDataCache[uid]?.avatar_url) {
            avatarLarge.src = userDataCache[uid].avatar_url;
            avatarLarge.style.display = "block";
        }

        fetchUserModal(uid, data => {
            loadingText.style.display = "none";

            if (!data) { extra.innerHTML = "Error loading user"; return; }

            const sourceHTML = `
                ${data.stored.source === "Seed List" ? `<span class="badge badge-seed">${data.stored.source}</span>` : `<span class="badge badge-new">Newly Added</span>`}
                ${data.is_star_creator ? '<span class="badge badge-star shiny">Star Creator</span>' : ''}
            `;

            header.innerHTML = `${data.stored.username} (${uid})`;
            pSource.innerHTML = sourceHTML;

            avatarLarge.src = data.avatar_url;
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
    });

    closeBtn.addEventListener("click", () => { modal.classList.remove("show"); });
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
    const loadingText = modal.querySelector(".loading-text");
    const header = modal.querySelector("h3");
    const pSource = modal.querySelector("p");
    const extra = modal.querySelector(".extra-info");
    const profileBtn = modal.querySelector(".profile-btn");

    // Initially hide avatar and show loading
    avatarLarge.style.display = "none";
    loadingText.style.display = "block";

    // Fetch full user data from your backend (like other users)
    fetch(`/user/${DEV_UID}`)
        .then(res => res.json())
        .then(data => {
            // Set card info dynamically
            devCard.querySelector("h3").innerText = data.stored.username || "Developer";
            devCard.querySelector("img.avatar-small").src = data.avatar_url || "";

            // Hide loading in card once avatar loaded
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

                avatarLarge.src = data.avatar_url;
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
            loadingText.style.display = "none";
        });

    closeBtn.addEventListener("click", () => modal.classList.remove("show"));
});

// ---------------- Initial batch avatar fetch ----------------
batchFetchAvatars();