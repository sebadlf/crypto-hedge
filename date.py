from datetime import datetime as dt

now = dt.utcnow()

other = now.replace(second=0, microsecond=0)

print(now)
print(other)