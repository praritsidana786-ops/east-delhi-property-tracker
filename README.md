# East Delhi Property Tracker

Daily 7 AM IST pe East Delhi ke 49 target areas mein listed nayi properties (₹1 Cr+, last 72 hours) ka digest aapke Telegram pe bhejta hai via GitHub Actions.

## Setup (pehli baar)

1. Settings → Secrets and variables → Actions → New repository secret
2. Add \"TELEGRAM_BOT_TOKEN\" with your bot token from BotFather
3. Add \"TELEGRAM_CHAT_ID\" with your chat id
4. Actions tab → enable workflows
5. Click \"Daily East Delhi Property Scan\" → \"Run workflow\" to test

## Config

- Cron: \"30 1 * * *\" = 01:30 UTC = 07:00 IST
- Min price: ₹1 Crore
- Window: last 72 hours
- 49 target areas (Anand Vihar, Preet Vihar, Vivek Vihar, Vigyan Vihar, Surya Niketan, Dayanand Vihar, Nirman Vihar, Surajmal Vihar, Preet Vihar, etc.)

## Sites scanned

MagicBricks, 99acres, NoBroker, SquareYards, PropertyWala, DreamProperty

## Modifications

- Budget: edit MIN_PRICE_INR in property_tracker.py
- Areas: edit TARGET_AREAS list
- Timing: edit cron in .github/workflows/daily-scrape.yml

## Honest limitations

99acres and MagicBricks use Cloudflare; some runs may return 0 listings. Playwright fallback is installed. Post-dates not always available on every site.
