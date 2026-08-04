const OperationBuilder =
    require("./OperationBuilder");

class BatchQueue {

    constructor() {

        this.queue = [];

    }

    // ----------------------------------
    // Add Single Operation
    // ----------------------------------

    enqueue(operation) {

        const builtOperation =
            OperationBuilder.build(operation);

        this.queue.push(
            builtOperation
        );

        return builtOperation;

    }

    // ----------------------------------
    // Add Multiple Operations
    // ----------------------------------

    enqueueMany(operations = []) {

        const builtOperations = [];

        for (const operation of operations) {

            builtOperations.push(

                this.enqueue(operation)

            );

        }

        return builtOperations;

    }

    // ----------------------------------
    // Remove First Operation (FIFO)
    // ----------------------------------

    dequeue() {

        if (this.isEmpty()) {

            return null;

        }

        return this.queue.shift();

    }

        // ----------------------------------
    // Peek First Operation
    // ----------------------------------

    peek() {

        if (this.isEmpty()) {

            return null;

        }

        return this.queue[0];

    }

    // ----------------------------------
    // Queue Size
    // ----------------------------------

    size() {

        return this.queue.length;

    }

    // ----------------------------------
    // Is Queue Empty
    // ----------------------------------

    isEmpty() {

        return this.queue.length === 0;

    }

    // ----------------------------------
    // Clear Queue
    // ----------------------------------

    clear() {

        this.queue = [];

    }

    // ----------------------------------
    // Get All Operations
    // ----------------------------------

    getAll() {

        return [...this.queue];

    }

}

module.exports =

    new BatchQueue();