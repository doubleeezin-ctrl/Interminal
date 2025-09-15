// ==UserScript==
// @name         Solscan Signatures Extractor (Discord Live)
// @namespace    local.solscan.extractor
// @version      1.0.0
// @description  Monitora o Discord Web, captura assinaturas de solscan.io/tx e envia para backend local, ignorando CSP via GM_xmlhttpRequest
// @author       you
// @match        https://discord.com/channels/*
// @run-at       document-end
// @grant        GM_xmlhttpRequest
// @connect      localhost
// @connect      127.0.0.1
// ==/UserScript==

(function () {
    'use strict';

    const DEFAULT_ENDPOINT = 'http://localhost:3000/tx';
    let currentEndpoint = DEFAULT_ENDPOINT;

    const seen = new Set();
    let observer = null;
    let queue = new Map();
    let timer = null;

    // Helpers
    function cleanText(s) { return String(s || '').replace(/[\u200B-\u200D\uFEFF]/g, '').trim(); }
    function sigFromHref(href) {
      try {
        const url = new URL(href, location.href);
        const m = url.pathname.match(/\/tx\/([A-Za-z0-9]+)(?:[\/?#]|$)/);
        return m && m[1] ? m[1] : null;
      } catch { return null; }
    }
    function fieldNameOf(block) {
      try {
        const el = block.querySelector('.embedFieldName__623de');
        return cleanText(el?.innerText || el?.textContent || '');
      } catch { return ''; }
    }
    function parseWalletLineFunding(anchorEl) {
      const li = anchorEl.closest('li') || anchorEl.parentElement;
      const text = cleanText(li?.textContent || '');
      const noTxn = text.replace(/\bTXN\b/gi, '').trim();
      const m = noTxn.match(/-\s*([^\n]+)$/);
      const fundOrigin = m ? cleanText(m[1]) : undefined;
      return { fundOrigin };
    }
    function parseRecentSwapFunding(anchorEl) {
      const title = cleanText(anchorEl.getAttribute('title') || '');
      const base = title.split('(')[0];
      const m = base.match(/^(.*?)\s+(\d+(?:\.\d+)?(?:s|m|h|d|w|mo|y))\b/i);
      if (m) return { fundOrigin: cleanText(m[1]), fundAge: cleanText(m[2]) };
      return {};
    }
    function extractEntries(root = document) {
      const results = new Map(); // Map<signature, { signature, fundOrigin?, fundAge?, tags?: string[] }>
      const fields = root.querySelectorAll('.embedField__623de');
      for (const block of fields) {
        const name = fieldNameOf(block);
        if (!/^(WALLETS|Recent Swaps)$/i.test(name)) continue;
        const isRS = /^Recent Swaps$/i.test(name);
        const anchors = block.querySelectorAll('a[href*="solscan.io/tx/"]');
        for (const a of anchors) {
          const sig = sigFromHref(a.href);
          if (!sig) continue;
          const tag = isRS ? 'RECENT_SWAPS' : 'WALLETS';
          const info = isRS ? parseRecentSwapFunding(a) : parseWalletLineFunding(a);
          const prev = results.get(sig);
          if (!prev) {
            results.set(sig, {
              signature: sig,
              ...(info.fundOrigin ? { fundOrigin: info.fundOrigin } : {}),
              ...(info.fundAge ? { fundAge: info.fundAge } : {}),
              tags: [tag],
            });
          } else {
            const tags = Array.from(new Set([...(prev.tags || []), tag]));
            let fundOrigin = prev.fundOrigin;
            let fundAge = prev.fundAge;
            if (tag === 'RECENT_SWAPS') {
              fundOrigin = info.fundOrigin || fundOrigin;
              fundAge = info.fundAge || fundAge;
            } else {
              if (!fundOrigin && info.fundOrigin) fundOrigin = info.fundOrigin;
              if (!fundAge && info.fundAge) fundAge = info.fundAge;
            }
            results.set(sig, { signature: sig, ...(fundOrigin ? { fundOrigin } : {}), ...(fundAge ? { fundAge } : {}), tags });
          }
        }
      }
      return [...results.values()];
    }

    function findMessageContainer(el) {
      if (!el || !(el instanceof Element)) return null;
      return (
        el.closest('[data-list-item-id^="chat-messages"]') ||
        el.closest('article[role="article"]') ||
        el.closest('li') ||
        el.closest('[class*="message"]') ||
        el.parentElement
      );
    }

    function sendViaGM(entries, endpoint = currentEndpoint) {
      return new Promise((resolve) => {
        if (!entries || entries.length === 0) {
          console.log('[extractor:tm] Nenhuma entrada encontrada.');
          return resolve();
        }
        console.log('[extractor:tm] Enviando', entries.length, 'entradas para', endpoint);
        GM_xmlhttpRequest({
          method: 'POST',
          url: endpoint,
          headers: { 'Content-Type': 'application/json' },
          data: JSON.stringify({ source: location.href, entries }),
          onload: (res) => {
            console.log('[extractor:tm] Resposta do backend:', res.status, res.responseText);
            resolve();
          },
          onerror: (err) => {
            console.error('[extractor:tm] Erro ao enviar para backend:', err);
            resolve();
          }
        });
      });
    }

    function enqueue(entries) {
      for (const e of entries) {
        const s = e?.signature;
        if (!s) continue;
        if (!seen.has(s)) {
          queue.set(s, e);
          seen.add(s);
        }
      }
      if (queue.size > 0 && !timer) {
        timer = setTimeout(async () => {
          const toSend = Array.from(queue.values());
          queue = new Map();
          timer = null;
          await sendViaGM(toSend);
        }, 1200);
      }
    }

    function startLive() {
      if (observer) {
        console.log('[extractor:tm] Live mode já está ativo.');
        return;
      }
      const target = document.body || document.documentElement;
      observer = new MutationObserver((mutations) => {
        for (const m of mutations) {
          for (const node of m.addedNodes || []) {
            if (!(node instanceof Element)) continue;
            const list = extractEntries(node);
            if (list.length) enqueue(list);
            if (node.tagName === 'A') {
              const list2 = extractEntries(node);
              if (list2.length) enqueue(list2);
            }
          }
        }
      });
      observer.observe(target, { childList: true, subtree: true });
      console.log('[extractor:tm] Live mode ON.');
    }

    function stopLive() {
      if (observer) {
        observer.disconnect();
        observer = null;
        console.log('[extractor:tm] Live mode OFF.');
      }
      if (timer) {
        clearTimeout(timer);
        timer = null;
      }
      queue.clear();
    }

    (async () => {
      // Varredura inicial
      const initial = extractEntries();
      initial.forEach((e) => { if (e.signature) seen.add(e.signature); });
      await sendViaGM(initial);
      // Live
      startLive();
    })();

    // Expor utilitários para o console
    try {
      // eslint-disable-next-line no-undef
      unsafeWindow.solscanExtractor = {
        startLive,
        stopLive,
        get isLive() { return !!observer; },
        setEndpoint(url) {
          currentEndpoint = url || DEFAULT_ENDPOINT;
          console.log('[extractor:tm] Endpoint atualizado para', currentEndpoint);
        },
      };
    } catch (_) {}
  })();


