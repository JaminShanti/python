import sys
import os

# Ensure the parent directory is in the path to import contact_bot
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from contact_bot import ContactBot

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def main():
    band_bot = ContactBot()
    keep_running = True
    first_time = True

    while keep_running:
        if first_time:
            first_time = False
        else:
            input("Press Enter to Continue...")
        
        clear_screen()
        print("1. Run Bot (Process All)")
        print("2. Run Sample (Process 'Sample Example')")
        print("3. Add a Contact")
        print("4. Display Records")
        print("9. Reload from File")
        print("10. Save to File")
        print("Type 'exit' to Stop")
        
        answer = input("What would you like to do? : ").strip()
        
        if answer == '':
            continue
        if answer.lower() == 'exit':
            keep_running = False
            break
            
        try:
            choice = int(answer)
        except ValueError:
            print("Invalid input. Please enter a number.")
            continue

        if choice == 1:
            band_bot.run()
        elif choice == 2:
            band_bot.run_sample()
        elif choice == 3:
            band_bot.add()
        elif choice == 4:
            band_bot.display()
        elif choice == 9:
            band_bot.reload()
        elif choice == 10:
            band_bot.save()
        else:
            print("Unknown option.")

    print("Exiting...")

if __name__ == "__main__":
    main()
