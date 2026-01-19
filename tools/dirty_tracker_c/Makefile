CC = gcc
CFLAGS = -Wall -Wextra -O2 -std=gnu11
LDFLAGS =

TARGET = dirty_tracker
SRC = main.c

.PHONY: all clean

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

clean:
	rm -f $(TARGET)
