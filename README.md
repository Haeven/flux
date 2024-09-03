MPEG-DASH, transcoding / transrating / sizing, TCP, HLS

Send and receive messages via Redpanda, send and recieve files via Gard. send client fetch messages to Redpanda and recieve them in Flux.

Flux is a TCP server that recieves messages from the client and sends them to Redpanda.

Flux sends cient download speed to gard, which identifies the best segment to send to the client.

Flux is the only service that can talk to the client.

Flux is also a client to the other services.

Flux can recieve a file via Client, and then store the file in Gard.