from datetime import timedelta

def weekDayGenerator(startDate, endDate):
    currentDate = startDate
    while currentDate < endDate:
        if currentDate.weekday() < 5:
            yield currentDate
        currentDate = currentDate + timedelta(days=1)