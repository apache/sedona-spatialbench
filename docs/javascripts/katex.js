(function () {
    function renderAll() {
        if (typeof renderMathInElement !== "function") return;
        renderMathInElement(document.body, {
            delimiters: [
                { left: "$$", right: "$$", display: true },
                { left: "$", right: "$", display: false },
                { left: "\\(", right: "\\)", display: false },
                { left: "\\[", right: "\\]", display: true }
            ],
            throwOnError: false
        });
    }

    renderAll();

    // Re-render on SPA page changes (mkdocs-material)
    if (window.document$) {
        document$.subscribe(renderAll);
    }
})();