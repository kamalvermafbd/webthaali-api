

class BatchQueue {

    constructor() {

        this.queue = [];

    }

    // ----------------------------------
    // Add Single Operation
    // ----------------------------------

    enqueue(operation) {

    if (!operation) {

        throw new Error(

            "Operation is required"

        );

    }

    this.queue.push(

        operation

    );

    return operation;

}
    // ----------------------------------
    // Add Multiple Operations
    // ----------------------------------

    enqueueMany(operations = []) {

        if (!Array.isArray(operations)) {

            throw new Error(

                "operations must be an array"

            );

        }

        const queuedOperations = [];

        for (const operation of operations) {

            queuedOperations.push(

                this.enqueue(operation)

            );

        }

        return queuedOperations;

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

        return true;

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