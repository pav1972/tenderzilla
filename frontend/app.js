document.addEventListener("DOMContentLoaded", () => {
  const API_BASE = "http://127.0.0.1:8000";
  const FILTERS_STORAGE_KEY = "tenderwise_sidebar_state";
  let tenders = [];

  const grid = document.getElementById("resultsGrid");
  const count = document.getElementById("resultsCount");
  const drawer = document.getElementById("drawer");
  const overlay = document.getElementById("overlay");
  const acceptableMinValueInput = document.getElementById("acceptableMinValue");
  const daysInput = document.getElementById("days");
  const sortSelect = document.getElementById("sortSelect");
  const updateBtn = document.getElementById("updateBtn");

  const tagEditorsConfig = {
    core_capabilities: {
      input: document.getElementById("coreCapabilitiesInput"),
      container: document.getElementById("coreCapabilitiesTags"),
    },
    secondary_capabilities: {
      input: document.getElementById("secondaryCapabilitiesInput"),
      container: document.getElementById("secondaryCapabilitiesTags"),
    },
    industry_focus: {
      input: document.getElementById("industryFocusInput"),
      container: document.getElementById("industryFocusTags"),
    },
    technologies_vendors: {
      input: document.getElementById("technologiesVendorsInput"),
      container: document.getElementById("technologiesVendorsTags"),
    },
    excluded_sectors: {
      input: document.getElementById("excludedSectorsInput"),
      container: document.getElementById("excludedSectorsTags"),
    },
    preferred_regions: {
      input: document.getElementById("preferredRegionsInput"),
      container: document.getElementById("preferredRegionsTags"),
    },
  };

  const tagState = {
    core_capabilities: [],
    secondary_capabilities: [],
    industry_focus: [],
    technologies_vendors: [],
    excluded_sectors: [],
    preferred_regions: [],
  };

  const safe = (x) => (x === null || x === undefined || x === "" ? "—" : x);

  const formatGBP = (v) => {
    const n = Number(v);
    return Number.isFinite(n) ? "£" + n.toLocaleString() : "—";
  };

  const formatDate = (value) => {
    if (!value) return "—";
    const d = new Date(value);
    if (Number.isNaN(d.getTime())) return value;
    return d.toLocaleDateString("en-GB", {
      day: "numeric",
      month: "long",
      year: "numeric",
    });
  };

  const scoreColor = (score) => {
    return score >= 50 ? "green" : "orange";
  };

  function normalizeTag(value) {
    return String(value || "")
      .replace(/\s+/g, " ")
      .trim();
  }

  function addTag(editorKey, rawValue) {
    const value = normalizeTag(rawValue);
    if (!value) return;

    const exists = tagState[editorKey].some(
      (tag) => tag.toLowerCase() === value.toLowerCase(),
    );
    if (exists) return;

    tagState[editorKey].push(value);
    renderTagEditor(editorKey);
    saveSidebarState();
  }

  function removeTag(editorKey, index) {
    tagState[editorKey].splice(index, 1);
    renderTagEditor(editorKey);
    saveSidebarState();
  }

  function renderTagEditor(editorKey) {
    const editor = tagEditorsConfig[editorKey];
    if (!editor?.container) return;

    editor.container.innerHTML = "";

    tagState[editorKey].forEach((tag, index) => {
      const chip = document.createElement("span");
      chip.className = "tag-chip";
      chip.innerHTML = `
        <span>${tag}</span>
        <button type="button" class="tag-remove" data-editor="${editorKey}" data-index="${index}">×</button>
      `;
      editor.container.appendChild(chip);
    });
  }

  function bindTagEditors() {
    Object.entries(tagEditorsConfig).forEach(([editorKey, editor]) => {
      if (!editor.input) return;

      editor.input.addEventListener("keydown", (e) => {
        if (e.key === "Enter" || e.key === ",") {
          e.preventDefault();
          addTag(editorKey, editor.input.value);
          editor.input.value = "";
        }

        if (e.key === "Backspace" && !editor.input.value.trim()) {
          if (tagState[editorKey].length > 0) {
            tagState[editorKey].pop();
            renderTagEditor(editorKey);
            saveSidebarState();
          }
        }
      });

      editor.input.addEventListener("blur", () => {
        const value = editor.input.value.trim();
        if (value) {
          addTag(editorKey, value);
          editor.input.value = "";
        }
      });
    });

    document.querySelectorAll(".tag-editor").forEach((el) => {
      el.addEventListener("click", (e) => {
        const removeBtn = e.target.closest(".tag-remove");
        if (removeBtn) {
          removeTag(removeBtn.dataset.editor, Number(removeBtn.dataset.index));
          return;
        }

        const editorKey = el.dataset.editor;
        const editor = tagEditorsConfig[editorKey];
        if (editor?.input) editor.input.focus();
      });
    });

    // Industry focus dropdown → tags
    const industryFocusSelect = document.getElementById("industryFocusSelect");

    if (industryFocusSelect) {
      industryFocusSelect.addEventListener("change", () => {
        const value = industryFocusSelect.value;

        if (value) {
          addTag("industry_focus", value);
          industryFocusSelect.value = "";
        }
      });
    }

    // Preferred regions dropdown → tags
    const preferredRegionsSelect = document.getElementById(
      "preferredRegionsSelect",
    );

    if (preferredRegionsSelect) {
      preferredRegionsSelect.addEventListener("change", () => {
        const value = preferredRegionsSelect.value;

        if (value) {
          addTag("preferred_regions", value);
          preferredRegionsSelect.value = "";
        }
      });
    }
  }

  function buildCompanyProfilePayload() {
    return {
      profile_name: "default",
      core_capabilities: [...tagState.core_capabilities],
      secondary_capabilities: [...tagState.secondary_capabilities],
      industry_focus: [...tagState.industry_focus],
      technologies_vendors: [...tagState.technologies_vendors],
      excluded_sectors: [...tagState.excluded_sectors],
      preferred_regions: [...tagState.preferred_regions],
      acceptable_min_tender_value: acceptableMinValueInput?.value
        ? Number(acceptableMinValueInput.value)
        : null,
      closing_within_days: daysInput?.value ? Number(daysInput.value) : null,
    };
  }

  function hasAnyProfileData() {
    const payload = buildCompanyProfilePayload();
    return (
      payload.core_capabilities.length > 0 ||
      payload.secondary_capabilities.length > 0 ||
      payload.industry_focus.length > 0 ||
      payload.technologies_vendors.length > 0 ||
      payload.excluded_sectors.length > 0 ||
      payload.preferred_regions.length > 0 ||
      payload.acceptable_min_tender_value !== null ||
      payload.closing_within_days !== null
    );
  }

  function hasSelectedCapabilities() {
    return (
      tagState.core_capabilities.length > 0 ||
      tagState.secondary_capabilities.length > 0 ||
      tagState.technologies_vendors.length > 0
    );
  }

  function getSidebarState() {
    return {
      tags: tagState,
      acceptableMinValue: acceptableMinValueInput?.value || "",
      days: daysInput?.value || "",
    };
  }

  function saveSidebarState() {
    try {
      localStorage.setItem(
        FILTERS_STORAGE_KEY,
        JSON.stringify(getSidebarState()),
      );
    } catch (err) {
      console.error("Failed to save sidebar state:", err);
    }
  }

  function restoreSidebarState() {
    try {
      const raw = localStorage.getItem(FILTERS_STORAGE_KEY);
      if (!raw) return;

      const state = JSON.parse(raw);

      if (state.tags) {
        Object.keys(tagState).forEach((key) => {
          tagState[key] = Array.isArray(state.tags[key]) ? state.tags[key] : [];
          renderTagEditor(key);
        });
      }

      if (acceptableMinValueInput) {
        acceptableMinValueInput.value = state.acceptableMinValue || "";
      }

      if (daysInput) {
        daysInput.value = state.days || "";
      }
    } catch (err) {
      console.error("Failed to restore sidebar state:", err);
    }
  }

  function bindSidebarPersistence() {
    if (acceptableMinValueInput) {
      acceptableMinValueInput.addEventListener("input", saveSidebarState);
    }

    if (daysInput) {
      daysInput.addEventListener("input", saveSidebarState);
    }
  }

  function formatCategory(code, description) {
    const safeCode = safe(code);
    const safeDescription = safe(description);

    if (safeCode !== "—" && safeDescription !== "—") {
      return `${safeCode} - ${safeDescription}`;
    }
    if (safeCode !== "—") {
      return safeCode;
    }
    if (safeDescription !== "—") {
      return safeDescription;
    }
    return "—";
  }

  const EXPLANATION_SCORE_BANDS = {
    excellentAt: 7.5,
    goodAt: 4,
  };

  function normalizeToTen(value, maxValue) {
    const numericValue = Number(value ?? 0);
    const numericMax = Number(maxValue ?? 0);

    if (
      !Number.isFinite(numericValue) ||
      !Number.isFinite(numericMax) ||
      numericMax <= 0
    ) {
      return 0;
    }

    const scaled = (numericValue / numericMax) * 10;
    return Math.max(0, Math.min(10, Math.round(scaled)));
  }

  function bandFromScore(outOfTen) {
    if (outOfTen >= EXPLANATION_SCORE_BANDS.excellentAt) return "Excellent";
    if (outOfTen >= EXPLANATION_SCORE_BANDS.goodAt) return "Good";
    return "Borderline";
  }

  function bandClass(band) {
    return band === "Borderline" ? "borderline" : "positive";
  }

  function joinEvidence(items, fallback = "") {
    const values = (Array.isArray(items) ? items : [])
      .map((item) => String(item || "").trim())
      .filter(Boolean);

    if (values.length === 0) return fallback;
    if (values.length === 1) return values[0];
    return `${values.slice(0, -1).join(", ")} and ${values[values.length - 1]}`;
  }

  function daysUntilDeadline(deadlineValue) {
    if (!deadlineValue) return null;

    const deadline = new Date(deadlineValue);
    if (Number.isNaN(deadline.getTime())) return null;

    const msPerDay = 24 * 60 * 60 * 1000;
    return Math.ceil((deadline.getTime() - Date.now()) / msPerDay);
  }

  function valueBand(tenderValue, minimumValue) {
    const value = Number(tenderValue ?? 0);
    const minimum = Number(minimumValue ?? 0);

    if (
      !Number.isFinite(value) ||
      value <= 0 ||
      !Number.isFinite(minimum) ||
      minimum <= 0
    ) {
      return null;
    }

    if (value < minimum) return "Borderline";
    if (value <= minimum * 2) return "Good";
    return "Excellent";
  }

  function deadlineBand(daysLeft) {
    if (daysLeft === null || !Number.isFinite(daysLeft)) return null;
    if (daysLeft <= 2) return "Borderline";
    if (daysLeft <= 7) return "Good";
    return "Excellent";
  }

  function renderExplanationIcon(icon) {
    const icons = {
      tools: `
        <svg viewBox="0 0 24 24" aria-hidden="true">
          <circle cx="12" cy="12" r="8.2" />
          <circle cx="12" cy="12" r="3.2" style="fill: currentColor; stroke: none;" />
        </svg>
      `,
      chip: `
        <svg viewBox="0 0 24 24" aria-hidden="true">
          <rect x="7" y="7" width="10" height="10" rx="2" />
          <path d="M9 3v4M15 3v4M9 17v4M15 17v4M3 9h4M3 15h4M17 9h4M17 15h4" />
        </svg>
      `,
      checklist: `
        <svg viewBox="0 0 24 24" aria-hidden="true">
          <path d="M8 7h11M8 12h11M8 17h11" />
          <path d="M4 7l1 1 2-2M4 12l1 1 2-2M4 17l1 1 2-2" />
        </svg>
      `,
      pin: `
        <svg viewBox="0 0 24 24" aria-hidden="true">
          <path d="M12 21s6-5.2 6-11a6 6 0 0 0-12 0c0 5.8 6 11 6 11z" />
          <circle cx="12" cy="10" r="2" />
        </svg>
      `,
      money: `
        <svg viewBox="0 0 24 24" aria-hidden="true">
          <rect x="4" y="7" width="16" height="10" rx="2" />
          <circle cx="12" cy="12" r="2" />
          <path d="M7 10v4M17 10v4" />
        </svg>
      `,
      clock: `
        <svg viewBox="0 0 24 24" aria-hidden="true">
          <circle cx="12" cy="12" r="8" />
          <path d="M12 8v5l3 2" />
        </svg>
      `,
    };

    return icons[icon] || icons.checklist;
  }

  // Converts the existing numeric breakdown into concise, human explanation cards.
  function buildExplanationCards(t) {
    const breakdown = t.pre_score_breakdown || {};
    const minimumValue = acceptableMinValueInput?.value;
    const tenderValue = t.value_amount ?? t.value;
    const daysLeft = daysUntilDeadline(t.deadline);

    const coreBand = bandFromScore(
      normalizeToTen(breakdown.core_capabilities, 16),
    );
    const techBand = bandFromScore(
      normalizeToTen(breakdown.technologies_vendors, 6),
    );
    const secondaryBand = bandFromScore(
      normalizeToTen(breakdown.secondary_capabilities, 8),
    );
    const locationBand = bandFromScore(normalizeToTen(breakdown.geography, 10));
    const valueFitBand =
      valueBand(tenderValue, minimumValue) ||
      bandFromScore(normalizeToTen(breakdown.value, 10));
    const windowBand =
      deadlineBand(daysLeft) ||
      bandFromScore(normalizeToTen(breakdown.deadline, 8));

    const coreEvidence = joinEvidence(t.matched_core_capabilities);
    const techEvidence = joinEvidence(t.matched_technologies_vendors);
    const secondaryEvidence = joinEvidence(t.matched_secondary_capabilities);
    const regionEvidence = joinEvidence(
      t.matched_regions,
      t.region ? String(t.region) : "the available geography",
    );

    return [
      {
        category: "core capabilities",
        band: coreBand,
        icon: "tools",
        text: coreEvidence
          ? `${coreEvidence} aligns with the tender’s main service requirements.`
          : "The core service fit is present, though the tender gives limited direct evidence.",
      },
      {
        category: "technologies / vendors",
        band: techBand,
        icon: "chip",
        text: techEvidence
          ? `${techEvidence} aligns with the stated technical environment.`
          : "No specific technology or vendor match is explicit, so this area should be checked manually.",
      },
      {
        category: "secondary capabilities",
        band: secondaryBand,
        icon: "checklist",
        text: secondaryEvidence
          ? `${secondaryEvidence} adds useful supporting relevance to the opportunity.`
          : "Supporting capabilities are not strongly evidenced, but this does not rule out a good fit.",
      },
      {
        category: "location",
        band: locationBand,
        icon: "pin",
        text: regionEvidence
          ? `The opportunity sits within ${regionEvidence}, which fits the preferred geography.`
          : "The location fit is not fully clear from the tender data provided.",
      },
      {
        category: "value",
        band: valueFitBand,
        icon: "money",
        text:
          valueFitBand === "Excellent"
            ? "The contract value is commercially attractive and sits well above the preferred minimum."
            : valueFitBand === "Good"
              ? "The contract value meets the preferred minimum and appears commercially workable."
              : "The contract value is below the preferred minimum and may need careful qualification.",
      },
      {
        category: "application window",
        band: windowBand,
        icon: "clock",
        text:
          windowBand === "Excellent"
            ? "There is a comfortable response window for a considered submission."
            : windowBand === "Good"
              ? "The submission window is workable, though planning should begin promptly."
              : "The submission timing is tighter than preferred and may require a quicker response.",
      },
    ];
  }

  function renderExplanationCard(card) {
    return `
      <div class="fit-card">
        <div class="fit-card-icon ${bandClass(card.band)}">
          ${renderExplanationIcon(card.icon)}
        </div>
        <div>
          <div class="fit-card-title">${card.band} on ${card.category}</div>
          <div class="fit-card-text">${safe(card.text)}</div>
        </div>
      </div>
    `;
  }

  async function loadTenders() {
    try {
      grid.innerHTML = "Loading...";

      if (!hasAnyProfileData()) {
        const response = await fetch(`${API_BASE}/tenders-all`);

        if (!response.ok) {
          throw new Error(`Failed to load tenders: ${response.status}`);
        }

        tenders = await response.json();
        applySort();
        return;
      }

      const payload = buildCompanyProfilePayload();

      const response = await fetch(`${API_BASE}/tenders-v2`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
      });

      if (!response.ok) {
        throw new Error(`Failed to load tenders: ${response.status}`);
      }

      tenders = await response.json();
      applySort();
    } catch (err) {
      console.error(err);
      grid.innerHTML = "Failed to load tenders.";
      count.innerText = "Results (0)";
    }
  }

  function render(data) {
    try {
      if (!grid || !count) return;

      grid.innerHTML = "";

      if (!data || data.length === 0) {
        count.innerText = "Results (0)";
        grid.innerHTML = "No tenders found.";
        return;
      }

      count.innerText = `Results (${data.length})`;

      const shouldShowMatch = hasSelectedCapabilities();

      data.forEach((t) => {
        const hasScore =
          shouldShowMatch &&
          t.pre_score_v2 !== null &&
          t.pre_score_v2 !== undefined;
        const score = hasScore ? t.pre_score_v2 : null;

        const card = document.createElement("div");
        card.className = "card";

        card.innerHTML = `
          <div class="card-top">
            <div>
              <div class="card-title">${safe(t.title)}</div>
            </div>

            ${
              hasScore
                ? `
              <div class="score ${scoreColor(score)}">
                <div class="score-value">${score}%</div>
                <div class="score-sub">Match</div>
              </div>
            `
                : ""
            }
          </div>

          <div class="card-buyer"><strong>Buyer:</strong> ${safe(t.buyer_name)}</div>

          <div
            class="card-description-wrap"
            style="position:relative; margin-top:12px;"
          >
            <div class="card-description">${safe(t.description)}</div>
            <button
              type="button"
              class="card-description-toggle"
              aria-expanded="false"
              style="display:none; position:absolute; right:0; bottom:0; padding:0 0 0 10px; border:none; background:linear-gradient(90deg, rgba(255,255,255,0), #fff 35%); color:#1e6bb8; cursor:pointer; font-size:15px; line-height:1.5;"
            >...</button>
          </div>

          <div class="card-meta">
            <div>
              <div class="card-meta-label">Notice type:</div>
              <div class="card-meta-value">${safe(t.latest_notice_type)}</div>
            </div>
            <div>
              <div class="card-meta-label">Value:</div>
              <div class="card-meta-value">${formatGBP(t.value_amount ?? t.value)}</div>
            </div>
            <div>
              <div class="card-meta-label">Category:</div>
              <div class="card-meta-value">${formatCategory(t.cpv_code, t.cpv_description)}</div>
            </div>
            <div>
              <div class="card-meta-label">Region:</div>
              <div class="card-meta-value">${safe(t.region)}</div>
            </div>
            <div>
              <div class="card-meta-label">Deadline:</div>
              <div class="card-meta-value">${formatDate(t.deadline)}</div>
            </div>
            <div>
              <div class="card-meta-label">OCID (Tender ID)</div>
              <div class="card-meta-value">${safe(t.ocid)}</div>
            </div>
          </div>

          <div class="card-action">
            <button class="ai-btn">${hasScore ? "View AI analysis →" : "View details →"}</button>
          </div>
        `;

        const btn = card.querySelector(".ai-btn");
        if (btn) btn.onclick = () => openDrawer(t);

        grid.appendChild(card);

        const descriptionEl = card.querySelector(".card-description");
        const descriptionToggleEl = card.querySelector(
          ".card-description-toggle",
        );

        if (descriptionEl && descriptionToggleEl) {
          requestAnimationFrame(() => {
            const isTruncated =
              descriptionEl.scrollHeight > descriptionEl.clientHeight + 1;

            if (!isTruncated) {
              return;
            }

            descriptionToggleEl.style.display = "inline-block";

            descriptionToggleEl.onclick = () => {
              const expanded =
                descriptionEl.getAttribute("data-expanded") === "true";

              if (expanded) {
                descriptionEl.setAttribute("data-expanded", "false");
                descriptionEl.style.display = "-webkit-box";
                descriptionEl.style.webkitBoxOrient = "vertical";
                descriptionEl.style.webkitLineClamp = "5";
                descriptionEl.style.lineClamp = "5";
                descriptionEl.style.overflow = "hidden";
                descriptionToggleEl.textContent = "...";
                descriptionToggleEl.setAttribute("aria-expanded", "false");
                descriptionToggleEl.style.position = "absolute";
                descriptionToggleEl.style.right = "0";
                descriptionToggleEl.style.bottom = "0";
                descriptionToggleEl.style.marginTop = "0";
                descriptionToggleEl.style.background =
                  "linear-gradient(90deg, rgba(255,255,255,0), #fff 35%)";
                return;
              }

              descriptionEl.setAttribute("data-expanded", "true");
              descriptionEl.style.display = "block";
              descriptionEl.style.webkitBoxOrient = "initial";
              descriptionEl.style.webkitLineClamp = "unset";
              descriptionEl.style.lineClamp = "unset";
              descriptionEl.style.overflow = "visible";
              descriptionToggleEl.textContent = "Show less";
              descriptionToggleEl.setAttribute("aria-expanded", "true");
              descriptionToggleEl.style.position = "static";
              descriptionToggleEl.style.marginTop = "6px";
              descriptionToggleEl.style.background = "none";
            };
          });
        }
      });
    } catch (err) {
      console.error("Render error:", err);
      if (grid) grid.innerHTML = "Something went wrong.";
    }
  }

  function renderPreScoreAnalysis(t) {
    const analysisEl = document.getElementById("analysis");
    if (!analysisEl) return;

    const score = t.pre_score_v2 ?? 0;
    const fitLabel =
      t.fit_band ||
      (score >= 75 ? "Strong fit" : score >= 50 ? "Moderate fit" : "Weak fit");
    const explanationCards = buildExplanationCards(t);

    analysisEl.innerHTML = `
      <h3>Match Score</h3>
      <h1 class="match-score-value ${scoreColor(score)}">${score}%</h1>
      <div class="fit-label">${safe(fitLabel)}</div>

      <hr>

      <h3>Fit</h3>
      <div class="fit-card-list">
        ${explanationCards.map(renderExplanationCard).join("")}
      </div>
    `;
  }

  function renderTenderDetails(t) {
    const analysisEl = document.getElementById("analysis");
    if (!analysisEl) return;

    analysisEl.innerHTML = `
      <h3>Tender details</h3>

      <div class="reason-block">
        <div class="reason-title">Notice type</div>
        <div class="reason-text">${safe(t.latest_notice_type)}</div>
      </div>

      <div class="reason-block">
        <div class="reason-title">Buyer</div>
        <div class="reason-text">${safe(t.buyer_name)}</div>
      </div>

      <div class="reason-block">
        <div class="reason-title">Category</div>
        <div class="reason-text">${formatCategory(t.cpv_code, t.cpv_description)}</div>
      </div>

      <div class="reason-block">
        <div class="reason-title">Region</div>
        <div class="reason-text">${safe(t.region)}</div>
      </div>

      <div class="reason-block">
        <div class="reason-title">Value</div>
        <div class="reason-text">${formatGBP(t.value_amount ?? t.value)}</div>
      </div>

      <div class="reason-block">
        <div class="reason-title">Deadline</div>
        <div class="reason-text">${formatDate(t.deadline)}</div>
      </div>

      <div class="reason-block">
        <div class="reason-title">Description</div>
        <div class="reason-text">${safe(t.description)}</div>
      </div>
    `;
  }

  function openDrawer(t) {
    if (!drawer || !overlay) return;

    saveSidebarState();

    drawer.classList.add("open");
    overlay.classList.add("show");

    const titleEl = document.getElementById("drawerTitle");
    const bodyEl = document.getElementById("drawerBody");
    const originalTenderUrl = t.submission_url || "";
    const hasScore =
      hasSelectedCapabilities() &&
      t.pre_score_v2 !== null &&
      t.pre_score_v2 !== undefined;

    if (titleEl) titleEl.innerText = safe(t.title);
    if (!bodyEl) return;

    bodyEl.innerHTML = `
      <div class="tabs">
        <button class="tab active" data-tab="analysis">${hasScore ? "AI Analysis" : "Tender details"}</button>
        <button class="tab" data-tab="requirements">Key Requirements</button>
        <button class="tab" data-tab="documents">Documents</button>
      </div>

      <div class="tab-content" id="analysis">
        <div style="padding:20px">Loading analysis...</div>
      </div>

      <div class="tab-content hidden" id="requirements">
        <div style="padding:20px">Coming soon.</div>
      </div>

      <div class="tab-content hidden" id="documents">
        <h3>Documents</h3>
        ${
          originalTenderUrl
            ? `<a href="${originalTenderUrl}" target="_blank" rel="noopener noreferrer">Open original tender</a>`
            : "No documents available"
        }
      </div>
    `;

    activateTabs();

    if (!hasScore) {
      renderTenderDetails(t);
      return;
    }

    renderPreScoreAnalysis(t);
  }

  function activateTabs() {
    document.querySelectorAll(".tab").forEach((tab) => {
      tab.onclick = () => {
        document
          .querySelectorAll(".tab")
          .forEach((t) => t.classList.remove("active"));
        tab.classList.add("active");

        document
          .querySelectorAll(".tab-content")
          .forEach((c) => c.classList.add("hidden"));

        const target = document.getElementById(tab.dataset.tab);
        if (target) target.classList.remove("hidden");
      };
    });
  }

  function closeDrawer() {
    if (drawer) drawer.classList.remove("open");
    if (overlay) overlay.classList.remove("show");
  }

  function bindHelpPopovers() {
    const helpButtons = document.querySelectorAll(".help-trigger");
    if (!helpButtons.length) return;

    const popover = document.createElement("div");
    popover.className = "help-popover hidden";
    popover.setAttribute("role", "tooltip");
    document.body.appendChild(popover);

    const closePopover = () => {
      popover.classList.add("hidden");
      helpButtons.forEach((button) => button.classList.remove("active"));
    };

    const positionPopover = (button) => {
      const rect = button.getBoundingClientRect();
      const gap = 8;
      const popoverWidth = popover.offsetWidth || 260;
      const left = Math.min(
        window.innerWidth - popoverWidth - 12,
        Math.max(12, rect.left),
      );

      popover.style.left = `${left + window.scrollX}px`;
      popover.style.top = `${rect.bottom + gap + window.scrollY}px`;
    };

    helpButtons.forEach((button) => {
      button.addEventListener("click", (event) => {
        event.stopPropagation();

        const isOpen =
          !popover.classList.contains("hidden") &&
          button.classList.contains("active");

        if (isOpen) {
          closePopover();
          return;
        }

        helpButtons.forEach((item) => item.classList.remove("active"));
        button.classList.add("active");
        popover.textContent = button.dataset.help || "";
        popover.classList.remove("hidden");
        positionPopover(button);
      });
    });

    popover.addEventListener("click", (event) => event.stopPropagation());
    document.addEventListener("click", closePopover);
    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape") closePopover();
    });
    window.addEventListener("resize", closePopover);
    window.addEventListener("scroll", closePopover, true);
  }

  if (overlay) overlay.onclick = closeDrawer;

  const closeBtn = document.getElementById("closeDrawer");
  if (closeBtn) closeBtn.onclick = closeDrawer;

  function applySort() {
    saveSidebarState();

    const mode = sortSelect?.value || "relevance";
    const shouldSortByScore = hasSelectedCapabilities();

    if (mode === "relevance") {
      const hasScores =
        shouldSortByScore &&
        tenders.some(
          (t) => t.pre_score_v2 !== null && t.pre_score_v2 !== undefined,
        );

      if (hasScores) {
        tenders.sort((a, b) => (b.pre_score_v2 ?? 0) - (a.pre_score_v2 ?? 0));
      } else {
        tenders.sort((a, b) =>
          (a.deadline || "").localeCompare(b.deadline || ""),
        );
      }
    } else {
      tenders.sort((a, b) =>
        (a.deadline || "").localeCompare(b.deadline || ""),
      );
    }

    render(tenders);
  }

  if (sortSelect) {
    sortSelect.onchange = () => applySort();
  }

  if (updateBtn) {
    updateBtn.onclick = () => {
      saveSidebarState();
      loadTenders();
    };
  }

  Object.keys(tagState).forEach((key) => renderTagEditor(key));
  restoreSidebarState();
  bindTagEditors();
  bindSidebarPersistence();
  bindHelpPopovers();
  loadTenders();
});
