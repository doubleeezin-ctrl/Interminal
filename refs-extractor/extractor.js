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
    let queue = new Set();
    let timer = null;
  
    function extractSolscanTxSignatures(root = document) {
      const signatures = new Set();
      const fields = root.querySelectorAll('.embedField__623de');
      for (const block of fields) {
        const nameEl = block.querySelector('.embedFieldName__623de');
        const name = (nameEl?.innerText || nameEl?.textContent || '').replace(/[\u200B-\u200D\uFEFF]/g, '').trim();
        if (!/^(WALLETS|Recent Swaps)$/i.test(name)) continue;
        const anchors = block.querySelectorAll('a[href*="solscan.io/tx/"]');
        for (const a of anchors) {
          try {
            const url = new URL(a.href, location.href);
            const m = url.pathname.match(/\/tx\/([A-Za-z0-9]+)(?:[\/?#]|$)/);
            if (m && m[1]) signatures.add(m[1]);
          } catch (_) {}
        }
      }
      return [...signatures];
    }
  
    function sendViaGM(signatures, endpoint = currentEndpoint) {
      return new Promise((resolve) => {
        if (!signatures || signatures.length === 0) {
          console.log('[extractor:tm] Nenhuma assinatura encontrada.');
          return resolve();
        }
        console.log('[extractor:tm] Enviando', signatures.length, 'assinatura(s) para', endpoint);
        GM_xmlhttpRequest({
          method: 'POST',
          url: endpoint,
          headers: { 'Content-Type': 'application/json' },
          data: JSON.stringify({ source: location.href, signatures }),
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
  
    function enqueue(signatures) {
      for (const s of signatures) {
        if (!seen.has(s)) {
          queue.add(s);
          seen.add(s);
        }
      }
      if (queue.size > 0 && !timer) {
        timer = setTimeout(async () => {
          const toSend = [...queue];
          queue.clear();
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
            const list = extractSolscanTxSignatures(node);
            if (list.length) enqueue(list);
            if (node.tagName === 'A') {
              const list2 = extractSolscanTxSignatures(node);
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
      const sigs = extractSolscanTxSignatures();
      sigs.forEach((s) => seen.add(s));
      await sendViaGM(sigs);
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
  
  
