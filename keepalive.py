import pyautogui
import time
import datetime

print("🔄 QMMX Keepalive script running...")

while True:
    pyautogui.move(1, 0)
    pyautogui.move(-1, 0)
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] 💡 Activity sent.")
    time.sleep(300)  # every 5 minutes (300 seconds)
