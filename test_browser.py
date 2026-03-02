"""Testa a página de vCenters com Playwright."""
from playwright.sync_api import sync_playwright
import time, json

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()

    # Captura todos os logs de console
    console_msgs = []
    page.on("console", lambda msg: console_msgs.append(f"[{msg.type}] {msg.text}"))

    # Captura erros de rede
    net_errors = []
    page.on("requestfailed", lambda req: net_errors.append(f"FAIL {req.url}"))

    # ── 1. Abre a página ────────────────────────────────────────────────────
    print("=== ABRINDO /vcenters ===")
    resp = page.goto("http://localhost:8000/vcenters", wait_until="domcontentloaded", timeout=15000)
    print(f"HTTP status da página: {resp.status}")
    time.sleep(4)

    # ── 2. Verifica ZH_API_KEY ─────────────────────────────────────────────
    api_key = page.evaluate("window.ZH_API_KEY")
    print(f"ZH_API_KEY injetada: {repr(api_key)}")

    # ── 3. Verifica o grid ─────────────────────────────────────────────────
    grid_html = page.locator("#zh-vc-grid").inner_html()
    has_spinner = "spinner-border" in grid_html
    has_cards   = "zh-vc-card" in grid_html
    has_empty   = "Nenhum vCenter" in grid_html
    print(f"Grid — spinner: {has_spinner} | cards: {has_cards} | msg vazia: {has_empty}")

    # ── 4. Testa fetch() direto do contexto do browser ─────────────────────
    print("\n=== TESTANDO FETCH /api/v1/vcenters NO BROWSER ===")
    result = page.evaluate("""
        async () => {
            const r = await fetch("/api/v1/vcenters");
            const body = await r.text();
            return { status: r.status, body: body.substring(0, 200) };
        }
    """)
    print(f"Status: {result['status']}")
    print(f"Body:   {result['body']}")

    # ── 5. Clica em "Novo vCenter" ─────────────────────────────────────────
    print("\n=== CLICANDO EM 'Novo vCenter' ===")
    btn = page.locator("button:has-text('Novo vCenter')").first
    if btn.count() == 0:
        print("BOTAO NAO ENCONTRADO!")
    else:
        print("Botão encontrado — clicando...")
        btn.click()
        time.sleep(1)

        modal_visible = page.locator("#zh-modal-vcenter").is_visible()
        print(f"Modal aberto: {modal_visible}")

        if modal_visible:
            # ── 6. Preenche o formulário ───────────────────────────────────
            print("\n=== PREENCHENDO FORMULÁRIO ===")
            page.fill("#zh-field-name",     "vc-playwright-test")
            page.fill("#zh-field-host",     "192.168.99.1")
            page.fill("#zh-field-port",     "443")
            page.fill("#zh-field-username", "admin@vsphere.local")
            page.fill("#zh-field-password", "SenhaTeste@123")
            print("Formulário preenchido")

            # ── 7. Clica em Salvar ─────────────────────────────────────────
            print("Clicando em Salvar...")
            page.click("#zh-btn-save")
            time.sleep(2)

            # Verifica se houve erro no formulário
            feedback = page.locator("#zh-form-feedback")
            if feedback.is_visible():
                print(f"Mensagem de feedback: {feedback.inner_text()}")
            else:
                print("Nenhuma mensagem de erro no formulário")

            # Verifica se o modal fechou (sucesso)
            modal_still_open = page.locator("#zh-modal-vcenter").is_visible()
            print(f"Modal ainda aberto após salvar: {modal_still_open}")

            if not modal_still_open:
                print("✓ SUCESSO! Modal fechou — vCenter cadastrado!")
                # Aguarda reload e verifica cards
                time.sleep(2)
                grid_html2 = page.locator("#zh-vc-grid").inner_html()
                print(f"Cards no grid após salvar: {'zh-vc-card' in grid_html2}")

    # ── 8. Erros de console ────────────────────────────────────────────────
    print("\n=== LOGS DO CONSOLE ===")
    errors_only = [m for m in console_msgs if "[error]" in m.lower()]
    if errors_only:
        for m in errors_only[:15]:
            print(m)
    else:
        print("Nenhum erro de console detectado.")

    print(f"\nTotal de logs: {len(console_msgs)}")
    for m in console_msgs[:20]:
        print(" ", m)

    browser.close()
    print("\n=== FIM DO TESTE ===")
