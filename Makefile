orchestra:
	go build -o orchestra Orchestra.go

node:
	go build -o node driverNode.go	

test_server:
	go run httpServer.go	

test_client:
	python3 app.py

clean:
	rm -f orchestra node Node_*.txt
	rm -r dato

all: orchestra node