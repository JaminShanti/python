import sys
import os

# Ensure the parent directory is in the path to import contact_bot
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from contact_bot import ContactBot

def main():
    try:
        band_bot = ContactBot()
        band_bot.run()
        print("Bot run completed successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
