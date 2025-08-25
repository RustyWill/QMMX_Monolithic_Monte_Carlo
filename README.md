# QMMX Monolithic v5

Desktop trading app with:
- Polygon.io live price & market status
- Level entry (Blue/Orange/Black/Teal; Solid/Dashed)
- Audit log with explicit reason codes
- Trade engine with stop/target and cooldown
- 40×1‑minute candlestick chart with level overlays
- Daily auto‑retraining (Logistic Regression) + Retrain Now

## Run
```
pip install -r requirements.txt
python qmmx_monolithic.py
```
- Save your Polygon API key in **Settings**.
- Enter and **Save Levels**.
- Start the engine on **Live**.
