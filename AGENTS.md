# hquant work notes

For Binance Spot Demo integration, read `docs/BINANCE_DEMO_VALIDATION.md` before testing or reporting completion. Keep its API status table and execution record current after each real Demo check. Local mock-exchange tests do not count as Binance Demo API validation.

Demo credentials belong only in the ignored, owner-only `hquant/local/binance_demo_credentials.yaml`. Never print credentials in test output or place them in tracked files. Trading modes are `pressure_sim` for the local pressure simulator and `online_sim` for Binance Spot Demo with virtual funds and fixed Demo endpoints. Production trading has no supported mode or endpoint configuration.
