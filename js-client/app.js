'use strict';

function DaffiClient(name) {
    this.name = name;
    this.pendingMessages = {};
    this.eventHandlers = [];
    this.closed = true;
    const self = this;

    this.connect = async (password) => {
        const module = await WebAssembly.compileStreaming(fetch("../../zig-out/lib/app.wasm"))
        const GeneratorFunctionT = (function* () {
            yield undefined;
        }).constructor;


        const decodeString = (pointer, length) => {
            const slice = new Uint8Array(
                memory.buffer,
                pointer,
                length
            )
            return new TextDecoder().decode(slice)
        }

        const encodeStringZ = (string) => {
            const buffer = new TextEncoder().encode(string)
            const pointer = allocUint8(buffer.length + 1)
            const slice = new Uint8Array(
                memory.buffer,
                pointer,
                buffer.length + 1
            )
            slice.set(buffer)
            slice[buffer.length] = 0
            return pointer
        }


        const encodeBuffer = (buffer) => {
            const len = buffer.byteLength;
            const pointer = allocUint8(len)
            const slice = new Uint8Array(
                memory.buffer,
                pointer,
                len
            )
            slice.set(buffer)
            return pointer
        }

        const encodeBufferZ = (buffer) => {
            const len = buffer.byteLength;
            const pointer = allocUint8(len)
            const slice = new Uint8Array(
                memory.buffer,
                pointer,
                len + 1
            )
            slice.set(buffer)
            slice[len] = 0
            return pointer
        }

        const {
            exports: {
                memory,
                allocUint8,
                free,
                sendHandshake,
                sendMessage,
                parseAndStoreMessage,
                initClient,
            },
        } = await WebAssembly.instantiate(module, {
            window: {
                _sendToSocket(pointer, length) {
                    const array = memory.buffer.slice(pointer, pointer + length);
                    socket.send(array);
                },
                _storeMessage(pointer, length, uuid, isError) {
                    const message = JSON.parse(decodeString(pointer, length));
                    const msgData = self.pendingMessages[uuid];
                    if (msgData && !Array.isArray(msgData)) {
                        // Resolve the pending promise
                        if (msgData.timeoutId) window.clearTimeout(msgData.timeoutId);
                        delete self.pendingMessages[uuid];
                        if (isError) {
                            if (message.args) {
                                msgData.reject(`Exception on remove side: "${message.args[0][0]}(${message.args[0][2]})"`);
                            } else {
                                msgData.reject(message);
                            }
                        } else msgData.resolve(message);
                    }
                    // Case when message arrived before the promise was created
                    else {
                        self.pendingMessages[uuid] = [message, isError];
                    }
                },
                _triggerEvent(pointer, length) {
                    const event = JSON.parse(decodeString(pointer, length));
                    const member = event.member;
                    if (event.type === "disconnected") {
                        // Reject all pending promises for the disconnected client
                        for (const msgData of Object.values(self.pendingMessages)) {
                            if (!Array.isArray(msgData) && msgData.receiver === member) {
                                if (msgData.timeoutId) window.clearTimeout(msgData.timeoutId);
                                delete self.pendingMessages[msgData.uuid];
                                msgData.reject(`Client "${member}" disconnected`);
                            }
                        }
                    }
                    for (const handler of self.eventHandlers) {
                        handler(event);
                    }
                },
            },
            env: {
                _throwError(pointer, length) {
                    throw new Error(decodeString(pointer, length));
                },
                _consoleLog(pointer, length) {
                    console.log(decodeString(pointer, length))
                },
            },
        })

        const socket = new WebSocket("ws://127.0.0.1:5000");
        socket.binaryType = "arraybuffer";

        socket.onmessage = (event) => {
            const buffer = new Uint8Array(event.data);
            const pointer = encodeBuffer(buffer);
            parseAndStoreMessage(pointer, buffer.byteLength, self.connNum); // TODO: use this approach for RAW messages
        };

        socket.onclose = (event) => {
            for (const msgData of Object.values(self.pendingMessages)) {
                if (!Array.isArray(msgData)) {
                    if (msgData.timeoutId) window.clearTimeout(msgData.timeoutId);
                    delete self.pendingMessages[msgData.uuid];
                    msgData.reject("Connection closed unexpectedly");
                }
            }
            self.closed = true;
        };

        const waitForOpenConnection = (socket) => {
            return new Promise((resolve, reject) => {
                const maxNumberOfAttempts = 10
                const intervalTime = 200 //ms
                let currentAttempt = 0
                const interval = setInterval(() => {
                    if (currentAttempt > maxNumberOfAttempts - 1) {
                        clearInterval(interval)
                        reject(new Error('Maximum number of attempts exceeded'))
                    } else if (socket.readyState === socket.OPEN) {
                        clearInterval(interval)
                        resolve()
                    }
                    currentAttempt++
                }, intervalTime)
            })
        }

        class Handler {

            constructor(receiver, timeout, serde, returnResult, connNum) {
                this.receiver = receiver;
                this.timeout = timeout;
                this.serde = serde;
                this.returnResult = returnResult;
                this.connNum = connNum;
            }

            get(target, prop) {
                let argsZ;
                const fn = async (...args) => {
                    if (this.serde === 1) {
                        argsZ = encodeStringZ(JSON.stringify({args: args, kwargs: {}}));
                    } else {
                        if (args.length === 1) {
                            if (!this.returnResult && args[0] instanceof GeneratorFunctionT) {
                                for (let arg of args[0]()) {
                                    fn(arg);
                                }
                                return undefined;
                            }
                            if (!(args[0] instanceof Uint8Array)) {
                                argsZ = encodeBufferZ(new TextEncoder().encode(args[0]));
                            } else argsZ = encodeBufferZ(args[0]);
                        } else throw new Error("Invalid arguments for raw message. Use a single buffer/string.");
                    }
                    const receiverZ = encodeStringZ(this.receiver);
                    const funcNameZ = encodeStringZ(prop);
                    const msgData = sendMessage(argsZ, receiverZ, funcNameZ, this.serde, this.returnResult, this.connNum);
                    if (this.returnResult) {
                        const view = new DataView(memory.buffer, msgData, 16);
                        const uuid = view.getUint32(0, true);
                        const ptr_len = view.getUint32(4, true);
                        const ptr = view.getUint32(8, true);
                        const isJson = view.getUint32(12, true);
                        const actualReceiver = decodeString(ptr, ptr_len);
                        if (this.timeout) {
                            const result = await this.getMessageByUuidWithTimeout(uuid, this.timeout, actualReceiver);
                            return isJson ? result.args[0] : result;
                        } else {
                            const result = await this.getMessageByUuid(uuid, actualReceiver);
                            return isJson ? result.args[0] : result;
                        }
                    }
                    free(msgData);
                }
                return fn;
            }

            getMessageByUuid(uuid, receiver) {
                return new Promise((resolve, reject) => {
                    const messageData = self.pendingMessages[uuid];
                    if (messageData && Array.isArray(messageData)) {
                        const [message, isError] = messageData;
                        delete self.pendingMessages[uuid];
                        if (isError) {
                            if (message.args) {
                                reject(`Exception on remove side: "${message.args[0][0]}(${message.args[0][2]})"`);
                            } else {
                                reject(message);
                            }
                        } else resolve(message);
                    }
                    self.pendingMessages[uuid] = {
                        resolve: resolve,
                        reject: reject,
                        timeoutId: null,
                        receiver: receiver
                    };
                });
            }

            getMessageByUuidWithTimeout(uuid, timeout, receiver) {
                return new Promise((resolve, reject) => {
                    let timeoutId;
                    timeoutId = setTimeout(() => {
                        delete self.pendingMessages[uuid];
                        window.clearTimeout(timeoutId);
                        reject('Operation timeout');
                    }, timeout);
                    const messageData = self.pendingMessages[uuid];
                    if (messageData && Array.isArray(messageData)) {
                        const [message, isError] = messageData;
                        delete self.pendingMessages[uuid];
                        window.clearTimeout(timeoutId);
                        if (isError) {
                            if (message.args) {
                                reject(`Exception on remove side: "${message.args[0][0]}(${message.args[0][2]})"`);
                            } else {
                                reject(message);
                            }
                        } else resolve(message);
                    }
                    self.pendingMessages[uuid] = {
                        resolve: resolve,
                        reject: reject,
                        timeoutId: timeoutId,
                        receiver: receiver
                    };
                });
            }
        }

        class Connection {
            constructor(connNum) {
                this.connNum = connNum;
            }

            rpc = (options) => {
                if (self.closed) throw new Error("Unable to send message on a closed connection");
                options = options || {};
                const receiver = options.receiver || "";
                const timeout = options.timeout || null;
                // 0 - "raw", 1 - "json"
                let serde;
                switch (options.serde || "json") {
                    case "json":
                    case 1:
                        serde = 1;
                        break;
                    case "raw":
                    case 0:
                        serde = 0;
                        break;
                    default:
                        throw new Error("Invalid serde option. use 'raw' or 'json'");
                }
                return new Proxy({}, new Handler(receiver, timeout, serde, true, this.connNum));
            };
            stream = (options) => {
                if (self.closed) throw new Error("Unable to send message on a closed connection");
                options = options || {};
                const receiver = options.receiver || "";
                // 0 - "raw", 1 - "json"
                let serde;
                switch (options.serde || "json") {
                    case "json":
                    case 1:
                        serde = 1;
                        break;
                    case "raw":
                    case 0:
                        serde = 0;
                        break;
                    default:
                        throw new Error("Invalid serde option. use 'raw' or 'json'");
                }
                return new Proxy({}, new Handler(receiver, null, serde, false, this.connNum));
            }
        }

        // Handshake process
        await waitForOpenConnection(socket);
        self.closed = false;
        const nameZ = encodeStringZ(self.name);
        const passwordZ = encodeStringZ(password);
        const connNum = initClient(nameZ);
        const uuid = sendHandshake(passwordZ, connNum);
        const handler = new Handler(null, null, 1, true, connNum);
        const handshake = await handler.getMessageByUuidWithTimeout(uuid, 5000, "");
        console.log(`Client "${self.name}" connected to the successfully! Connection type: ${handshake.meta.type}`);
        return new Connection(connNum);
    }

    // Subscribe to server events
    this.addEventHandler = (eventHandler) => self.eventHandlers.push(eventHandler);
}
