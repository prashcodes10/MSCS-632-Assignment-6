import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class Task {
    private final int taskId;
    private final String data;

    public Task(int taskId, String data) {
        this.taskId = taskId;
        this.data = data;
    }

    public int getTaskId() {
        return taskId;
    }

    public String getData() {
        return data;
    }
}

class SharedTaskQueue {
    private final Queue<Task> queue = new LinkedList<>();

    public synchronized void addTask(Task task) {
        queue.add(task);
    }

    public synchronized Task getTask() {
        return queue.poll();
    }
}

class ResultWriter {
    private final String fileName;

    public ResultWriter(String fileName) {
        this.fileName = fileName;
    }

    public synchronized void writeResult(String result) {
        try (FileWriter writer = new FileWriter(fileName, true)) {
            writer.write(result + "\n");
        } catch (IOException e) {
            System.out.println("File writing error: " + e.getMessage());
        }
    }
}

class Worker implements Runnable {
    private final int workerId;
    private final SharedTaskQueue taskQueue;
    private final ResultWriter resultWriter;

    public Worker(int workerId, SharedTaskQueue taskQueue, ResultWriter resultWriter) {
        this.workerId = workerId;
        this.taskQueue = taskQueue;
        this.resultWriter = resultWriter;
    }

    @Override
    public void run() {
        System.out.println("Worker " + workerId + " started.");

        while (true) {
            Task task = taskQueue.getTask();

            if (task == null) {
                System.out.println("Worker " + workerId + " found no more tasks and is stopping.");
                break;
            }

            try {
                System.out.println("Worker " + workerId + " processing Task " + task.getTaskId());

                Thread.sleep(1000);

                String result = "Worker " + workerId + " completed Task " 
                        + task.getTaskId() + " with data: " + task.getData();

                resultWriter.writeResult(result);
                System.out.println(result);

            } catch (InterruptedException e) {
                System.out.println("Worker " + workerId + " was interrupted: " + e.getMessage());
                Thread.currentThread().interrupt();
                break;
            }
        }

        System.out.println("Worker " + workerId + " completed execution.");
    }
}

public class Main {
    public static void main(String[] args) {
        SharedTaskQueue taskQueue = new SharedTaskQueue();
        ResultWriter resultWriter = new ResultWriter("java_output.txt");

        for (int i = 1; i <= 10; i++) {
            taskQueue.addTask(new Task(i, "Data item " + i));
        }

        int numberOfWorkers = 3;
        ExecutorService executor = Executors.newFixedThreadPool(numberOfWorkers);

        for (int i = 1; i <= numberOfWorkers; i++) {
            executor.execute(new Worker(i, taskQueue, resultWriter));
        }

        executor.shutdown();

        while (!executor.isTerminated()) {
            // Waiting for all workers to finish
        }

        System.out.println("All Java tasks processed successfully.");
    }
}