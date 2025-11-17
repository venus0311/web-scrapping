(function () {
    'use strict';
  
    // Helper: safe get select values (works with native select or Select2)
    function getSelectValues(selectEl) {
      if (!selectEl) return [];
      // If it's select2, the underlying select still contains the selected options
      if (selectEl.multiple) {
        return Array.from(selectEl.selectedOptions).map(o => o.value);
      } else {
        const v = selectEl.value;
        return v === '' || v == null ? [] : [v];
      }
    }
  
    // Initialize a single global select (by selector) with "All" handling
    function initSingleSelectWithAll(selector, placeholderText) {
      if (typeof $ === 'undefined' || !$.fn || !$.fn.select2) {
        console.warn('Select2 not loaded - skipping select2 init for', selector);
        return;
      }
      const $sel = $(selector);
      if ($sel.length === 0) return;
  
      // If already initialized, destroy to avoid duplicates
      if ($sel.hasClass('select2-hidden-accessible')) {
        try { $sel.select2('destroy'); } catch (e) {}
      }
  
      $sel.select2({
        placeholder: placeholderText || ($sel.attr('data-placeholder') || ''),
        allowClear: true
      });
  
      // namespaced handlers so we can remove them safely
      $sel.off('select2:select.selectAllHandler').on('select2:select.selectAllHandler', function (e) {
        if (e && e.params && e.params.data && e.params.data.id === 'all') {
          // pick all values except 'all' and 'any'
          const allOptions = $sel.find('option:not([value="all"]):not([value="any"])').map(function () {
            return $(this).val();
          }).get();
          $sel.val(allOptions).trigger('change');
        }
      });
  
      $sel.off('select2:unselect.selectAllHandler').on('select2:unselect.selectAllHandler', function (e) {
        if (e && e.params && e.params.data && e.params.data.id === 'all') {
          $sel.val(null).trigger('change');
        }
      });
    }


    function initSelectWithAllDynamic(container) {
        if (typeof $ === 'undefined' || !$.fn || !$.fn.select2) return;
      
        const $container = $(container);
        $container.find('select').each(function () {
          const $el = $(this);
      
          // 1) Add a class to the original <select> so you can always target it
          $el.addClass('req-box-select');
      
          // 2) Add semantic classes depending on id/name (level2 = "from", level3 = "to")
          const id = $el.attr('id') || '';
          const name = $el.attr('name') || '';
          if (/^level2(?:_|$)/.test(id) || name === 'level2[]') $el.addClass('level-from');
          if (/^level3(?:_|$)/.test(id) || name === 'level3[]') $el.addClass('level-to');
      
          // Destroy any existing select2 instance (safe re-init)
          if ($el.hasClass('select2-hidden-accessible')) {
            try { $el.select2('destroy'); } catch (e) { /* ignore */ }
          }
      
          // Prepare classes to pass into Select2 (so the visible container gets them)
          let containerClasses = 'req-box-select-container';
          if ($el.hasClass('level-from')) containerClasses += ' level-from-container';
          if ($el.hasClass('level-to')) containerClasses += ' level-to-container';
      
          $el.select2({
            placeholder: $el.attr('data-placeholder') || $el.attr('placeholder') || '',
            allowClear: true,
            containerCssClass: containerClasses, // class on the visible container
            dropdownCssClass: 'req-box-select-dropdown'
          });
      
          // Fallback: ensure the created container actually has the classes
          const s2 = $el.data('select2');
          if (s2 && s2.$container) s2.$container.addClass(containerClasses);
      
          // your existing select-all handlers (unchanged)
          $el.off('select2:select.selectAllHandler').on('select2:select.selectAllHandler', function (e) {
            if (e?.params?.data?.id === 'all') {
              const allOptions = $el.find('option:not([value="all"]):not([value="any"])')
                                .map(function () { return $(this).val(); }).get();
              $el.val(allOptions).trigger('change');
            }
          });
      
          $el.off('select2:unselect.selectAllHandler').on('select2:unselect.selectAllHandler', function (e) {
            if (e?.params?.data?.id === 'all') {
              $el.val(null).trigger('change');
            }
          });
        });
      }
      
  
    // Poll / display functions: copied mostly as-is from your original script
    async function fetchEntries() {
      try {
        const response = await fetch('/api/entries');
        if (!response.ok) throw new Error('Failed to fetch entries');
  
        const data = await response.json();
        const tbody = document.getElementById('entries-table-body');
        if (!tbody) return;
  
        tbody.innerHTML = '';
  
        if (data.length === 0) {
          tbody.innerHTML = '<tr><td colspan="8">No sheets processed yet.</td></tr>';
          return;
        }
  
        data.forEach((entry, index) => {
          const row = document.createElement('tr');
  
          let controlButton = '-';
          if (entry.status === "Stopped" || entry.status === "Failed") {
            controlButton = `
              <form method="post" action="/resume/${entry.id}" style="display:inline;">
                <button type="submit" class="resume-btn" title="Resume"><i class="fas fa-play"></i></button>
              </form>
            `;
          } else if (entry.status === "In Progress") {
            controlButton = `
              <form method="post" action="/stop/${entry.id}" style="display:inline;">
                <button type="submit" class="stop-btn" title="Stop"><i class="fas fa-pause"></i></button>
              </form>
            `;
          }
  
          row.innerHTML = `
            <td>${index + 1}</td>
            <td class="truncate-text" title="${entry.name}">${entry.name}</td>
            <td><a href="${entry.url}" target="_blank" class="url-cell" title="${entry.url}">${entry.url}</a></td>
            <td>
              <span class="${
                entry.status.toLowerCase() === 'done' ? 'status-done' :
                entry.status.toLowerCase() === 'failed' ? 'status-failed' :
                entry.status.toLowerCase() === 'stopped' ? 'status-stopped' :
                'status-progress'
              }">
                ${entry.status}
              </span>
            </td>
            <td title="${entry.error_message || ''}">${entry.error_message || '-'}</td> 
            <td>${controlButton}</td>
            <td>
              <form method="post" action="/delete/${entry.id}" style="display:inline;">
                <button type="submit" class="delete-btn" title="Delete"><i class="fas fa-trash"></i></button>
              </form>
            </td>
          `;
  
          tbody.appendChild(row);
        });
      } catch (error) {
        console.error('Error refreshing entries:', error);
      }
    }
  
    async function deleteAllEntries() {
      if (!confirm("Are you sure you want to delete ALL entries?")) return;
  
      try {
        const response = await fetch('/api/delete-all', { method: 'POST' });
        if (!response.ok) throw new Error('Failed to delete all entries');
  
        fetchEntries();
        alert('All entries deleted successfully.');
      } catch (err) {
        alert('Error deleting all entries: ' + err.message);
      }
    }
  
    document.addEventListener('click', async (e) => {
      if (e.target.closest('.stop-btn')) {
        e.preventDefault();
        const form = e.target.closest('form');
        const action = form.action;
  
        try {
          const res = await fetch(action, { method: 'POST' });
          const data = await res.json();
  
          // if (data.success) {
          //   alert('Process stopped!');
          //   fetchEntries();
          // } else {
          //   alert('Failed to stop process: ' + data.error);
          // }
        } catch (err) {
          console.error(err);
        }
      } else if (e.target.closest('.resume-btn')) {
        e.preventDefault();
        const form = e.target.closest('form');
        const action = form.action;
  
        try {
          const res = await fetch(action, { method: 'POST' });
          const data = await res.json();
  
          // if (data.success) {
          //   alert('Process resumed!');
          //   fetchEntries();
          // } 
          // else {
          //   alert('Failed to resume: ' + data.error);
          // }
        } catch (err) {
          console.error(err);
        }
      }
    });
  
    // Polling + display processed data code (unchanged logic)
    async function pollForProcessedData(entryId, retries = 30, delay = 4000) {
      console.log(`🔍 Starting to poll for entry: ${entryId}`);
  
      for (let i = 0; i < retries; i++) {
        try {
          const response = await fetch(`/api/process-data/${entryId}`);
  
          if (response.status === 404) {
            console.log(`⌛ Data not ready yet (attempt ${i + 1}/${retries})`);
            await new Promise(resolve => setTimeout(resolve, delay));
            continue;
          }
  
          if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
          }
  
          const data = await response.json();
  
          if (data.success) {
            console.log('✅ Processed data received:', data.data);
            displayProcessedData(data.data);
            return;
          } else {
            console.log('❌ API returned unsuccessful response:', data);
          }
  
        } catch (error) {
          console.error('Error fetching processed data:', error);
        }
  
        await new Promise(resolve => setTimeout(resolve, delay));
      }
  
      console.log('⏰ Processed data not available within timeout');
    }
  
    function displayProcessedData(data) {
      const container = document.getElementById('processedDataContainer');
      if (!container) {
        console.error('Processed data container not found!');
        return;
      }
  
      container.style.display = 'block';
  
      const levelsContainer = document.getElementById('jobLevelsTags');
      if (levelsContainer && data.job_levels) {
        levelsContainer.innerHTML = data.job_levels.map(level =>
          `<span class="data-tag">${level}</span>`
        ).join('');
      }
  
      const functionsContainer = document.getElementById('jobFunctionsTags');
      if (functionsContainer && data.job_functions) {
        functionsContainer.innerHTML = data.job_functions.map(func =>
          `<span class="data-tag">${func}</span>`
        ).join('');
      }
  
      const keywordsContainer = document.getElementById('keywordsTags');
      if (keywordsContainer && data.keywords) {
        keywordsContainer.innerHTML = data.keywords.map(keyword =>
          `<span class="data-tag">${keyword}</span>`
        ).join('');
      }
  
      const geoContainer = document.getElementById('geoTags');
      if (geoContainer && data.geo_locations) {
        let geoArray;
        if (Array.isArray(data.geo_locations)) {
          geoArray = data.geo_locations;
        } else if (typeof data.geo_locations === 'string') {
          try {
            geoArray = JSON.parse(data.geo_locations);
          } catch (e) {
            geoArray = data.geo_locations.split(',').map(item => item.trim());
          }
        } else {
          geoArray = [data.geo_locations];
        }
  
        geoContainer.innerHTML = geoArray.map(geo =>
          `<span class="data-tag">${geo}</span>`
        ).join('');
      } else if (geoContainer) {
        geoContainer.innerHTML = '<span class="data-tag">No geo data</span>';
      }
  
      const timestampEl = document.getElementById('processedTimestamp');
      const entryNameEl = document.getElementById('processedEntryName');
  
      if (timestampEl && data.processed_at) {
        timestampEl.textContent = new Date(data.processed_at).toLocaleString();
      }
  
      if (entryNameEl && data.entry_name) {
        entryNameEl.textContent = data.entry_name;
      }
    }
  
    function clearProcessedData() {
      const container = document.getElementById('processedDataContainer');
      if (container) container.style.display = 'none';
  
      ['jobLevelsTags', 'jobFunctionsTags', 'keywordsTags', 'geoTags'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.innerHTML = '';
      });
  
      const timestampEl = document.getElementById('processedTimestamp');
      const entryNameEl = document.getElementById('processedEntryName');
  
      if (timestampEl) timestampEl.textContent = '';
      if (entryNameEl) entryNameEl.textContent = '';
    }
  
    // ---------- Dynamic requirements box logic ----------
    document.addEventListener('DOMContentLoaded', function initAll() {
      console.log('✅ Script loaded');
  
      // Find requirements container & template
      const firstBox = document.querySelector('.requirements_box');
      if (!firstBox) {
        console.warn('No .requirements_box found in DOM. Dynamic requirement boxes disabled.');
        // still continue to initialize other things (entries, form handlers)
      }
  
      // Save a raw template before any Select2 initialization (so we clone a clean copy)
      const templateBox = firstBox ? firstBox.cloneNode(true) : null;
  
      // container where boxes live (parent)
      const requirementsContainer = firstBox ? firstBox.parentNode : null;
  
      // counter = number of existing boxes
      let reqBoxCounter = document.querySelectorAll('.requirements_box').length || 0;
  
      // Initialize global selects (outside requirements_box) if present
      initSingleSelectWithAll('#industry', 'Select industry');

      initSingleSelectWithAll('#country', 'Select geolocation');
  
      // Initialize select2 for each existing requirements box
      document.querySelectorAll('.requirements_box').forEach(box => initSelectWithAllDynamic(box));
  
      // Add button handler (clone template and initialize)
      const addButton = document.querySelector('.add_req_button');

      if (addButton && templateBox && requirementsContainer) {
        addButton.addEventListener('click', function (e) {
          e.preventDefault();
  
          reqBoxCounter += 1;
  
          // Clone the clean template (no select2 wrappers)
          const newBox = templateBox.cloneNode(true);
  
          // Build mapping oldId -> newId for elements inside newBox
          const idMap = {};
          Array.from(newBox.querySelectorAll('[id]')).forEach(el => {
            const oldId = el.id;
            // base name (strip trailing _N if exists)
            const base = oldId.replace(/_\d+$/, '');
            const newId = `${base}_${reqBoxCounter}`;
            idMap[oldId] = newId;
            el.id = newId;
  
            // clear values
            if (el.tagName === 'SELECT') {
              try { el.selectedIndex = -1; } catch (e) {}
            } else if (el.tagName === 'TEXTAREA' || el.tagName === 'INPUT') {
              el.value = '';
            }
          });
  
          // Update attributes referencing old ids (labels, aria-*, anchors)
          Object.keys(idMap).forEach(oldId => {
            const newId = idMap[oldId];
  
            // labels
            Array.from(newBox.querySelectorAll(`label[for="${oldId}"]`)).forEach(l => l.setAttribute('for', newId));
  
            // aria-describedby / aria-labelledby
            Array.from(newBox.querySelectorAll(`[aria-describedby="${oldId}"], [aria-labelledby="${oldId}"]`)).forEach(el => {
              if (el.getAttribute('aria-describedby') === oldId) el.setAttribute('aria-describedby', newId);
              if (el.getAttribute('aria-labelledby') === oldId) el.setAttribute('aria-labelledby', newId);
            });
  
            // anchors pointing to ids
            Array.from(newBox.querySelectorAll(`a[href="#${oldId}"]`)).forEach(a => a.setAttribute('href', `#${newId}`));
          });
  
          // Append and initialize select2 inside the new box
          requirementsContainer.appendChild(newBox);
          initSelectWithAllDynamic(newBox);
  
          // (optional) bring focus to first input in the new box
          const firstInput = newBox.querySelector('select, textarea, input, button');
          if (firstInput) firstInput.focus();
        });
      }
  
      // ---------- Form submit: collect dynamic requirement boxes ----------
      const form = document.getElementById('processForm');

      if (!form) {
        console.error('Form not found!');
        // still begin periodic fetchEntries
      } else {
        form.addEventListener('submit', async function (e) {
          e.preventDefault();
          clearProcessedData();
  
          // Global/top-level fields
          let selectedValue = document.querySelector('input[name="process_type"]:checked');
          let process_type = selectedValue ? selectedValue.value : null; 
          const exclude_keywords = document.getElementById('exclude_keywords')?.value || '';
          const sheet_url = document.getElementById('sheet_url')?.value || '';
          const sup_emails_sheet_url = document.getElementById('sup_emails_sheet_url')?.value || '';
          const sup_domains_sheet_url = document.getElementById('sup_domains_sheet_url')?.value || '';
          const sup_names_sheet_url = document.getElementById('sup_names_sheet_url')?.value || '';
          const goal = document.getElementById('goal')?.value || '';
          const lpc = document.getElementById('lpc')?.value || '';
          const size = document.getElementById('size')?.value || '';
          const revenue = document.getElementById('revenue')?.value || '';
          const company_geo = document.getElementById("company_geo_sw")?.checked || false;
  
          // industry global select: use native select element if available
          const industrySelect = document.getElementById('industry');
          const industry = industrySelect ? getSelectValues(industrySelect) : [];
          
          // geolocation global select: use native select element if available
          const countrySelect = document.getElementById('country');
          const geo = countrySelect ? getSelectValues(countrySelect) : [];
  
          // Collect each requirements_box into an object
          const requirements = Array.from(document.querySelectorAll('.requirements_box')).map(box => {
            const job_function_select = box.querySelector('select[name="function[]"]') || box.querySelector('select[id^="function"]');
            const level1_select = box.querySelector('select[name="level[]"]') || box.querySelector('select[id^="level_"]');
            const level2_select = box.querySelector('select[name="level2[]"]') || box.querySelector('select[id^="level2_"]');
            const level3_select = box.querySelector('select[name="level3[]"]') || box.querySelector('select[id^="level3_"]');
            const keywords_el = box.querySelector('textarea[name="keywords"]') || box.querySelector('.keywords_text_area');
  
            return {
              job_function: job_function_select ? getSelectValues(job_function_select) : [],
              level1: level1_select ? getSelectValues(level1_select) : [],
              level2: level2_select ? getSelectValues(level2_select) : [],
              level3: level3_select ? getSelectValues(level3_select) : [],
              keywords: keywords_el ? (keywords_el.value || '').trim() : '',
            };
          });
  
          try {
            const res = await fetch('/process', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                geo,
                exclude_keywords,
                sheet_url,
                company_geo,
                sup_emails_sheet_url,
                sup_domains_sheet_url,
                sup_names_sheet_url,
                goal,
                lpc,
                size,
                industry,
                revenue,
                requirements,
                process_type
              })
            });
  
            // read text first (to detect redirect/html)
            const responseText = await res.text();
  
            if (responseText.startsWith('<!DOCTYPE') || responseText.startsWith('<html')) {
              console.log('Redirect detected, proceeding to dashboard');
              window.location.href = '/dashboard';
              return;
            }
  
            try {
              const result = JSON.parse(responseText);
  
              if (!res.ok) {
                if (result.error === 'no_edit') {
                  window.location.href = '/dashboard?error=no_edit';
                }
                return;
              }
  
              console.log('✅ Process result:', result);
  
              if (result.entry_id) {
                pollForProcessedData(result.entry_id);
              }
  
            } catch (jsonError) {
              console.error('Failed to parse JSON response:', jsonError);
              window.location.href = '/dashboard';
            }
  
          } catch (err) {
            console.error('❌ Network error:', err);
            window.location.href = '/dashboard';
          }
        });
      }
  
      // Start fetching entries and polling periodically
      fetchEntries();
      setInterval(fetchEntries, 5000);

      window.deleteAllEntries = deleteAllEntries;

    }); // end DOMContentLoaded init
  
  })();
  