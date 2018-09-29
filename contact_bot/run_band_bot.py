import sys
sys.path.append("..")
from contact_bot import ContactBot
band_bot = ContactBot()
keep_running = True
first_time = True
while keep_running:
    if first_time:
        first_time = False
    else:
        raw_input("Press Enter to Continue")
    print chr(27) + "[2J"
    print "Choose 1 to Run."
    print "Choose 2 to Run Sample."
    print "Choose 3 to Add a Row."
    print "Choose 4 to Diplay Records"
    print "Choose 9 to Reload from File."
    print "Chhose 10 to Save."
    print "Choose exit to Stop."
    answer =  raw_input("What would you like to do? : ")
    if answer == '':
        continue
    if answer == 'exit':
        keep_running = False
        break
    if int(answer) == 1:
        band_bot.run()
    if int(answer) == 2:
        band_bot.run_sample()
    if int(answer) == 3:
        band_bot.add()
    if int(answer) == 4:
        band_bot.display()
    if int(answer) == 9:
        band_bot.reload()
    if int(answer) == 10:
        band_bot.save()
print "Exiting..."
