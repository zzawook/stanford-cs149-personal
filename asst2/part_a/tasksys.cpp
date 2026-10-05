#include "tasksys.h"
#include <algorithm>
#include <thread>
#include <latch>
#include <atomic>
#include <condition_variable>

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    threads = new std::thread[num_threads];
    this->num_threads = num_threads;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {
    delete[] threads;
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    int total_threads = std::min(this->num_threads, num_total_tasks);
    int num_threads = this->num_threads;

    for (int i = 0; i < total_threads; i++) {
        threads[i] = std::thread([runnable, i, num_total_tasks, num_threads] {
            int per_thread = std::max(1, num_total_tasks / num_threads);
            int iters = per_thread;
            if (i == num_threads - 1) {
                iters += num_total_tasks % num_threads;
            }
            for (int j = 0; j < iters; j++) {
                runnable->runTask((i * per_thread) + j, num_total_tasks);
            }
        });
    }

    for (int i = 0; i < total_threads; i++) {
        threads[i].join();
    }

}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

Task::Task(IRunnable* runnable, int i, int num_total_tasks, std::latch* latch) {
    this->runnable = runnable;
    this->i = i;
    this->num_total_tasks = num_total_tasks;
    this->latch = latch;
}

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    this->thread_pool = new std::thread[num_threads];
    this->num_threads = num_threads;
    for (int i = 0; i < num_threads; i++) {
        this->thread_pool[i] = std::thread([this] {
            while (this->cont) {
                std::unique_lock<std::mutex> lock(this->queue_mtx);
                if (!this->q.empty()) {
                    Task task = this->q.front();
                    this->q.pop();
                    lock.unlock();

                    task.runnable->runTask(task.i, task.num_total_tasks);
                    task.latch->count_down();
                } else {
                    lock.unlock();
                
                }
            } 
        });
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    this->cont = false;
    for (int i = 0; i < this->num_threads; i++) {
        this->thread_pool[i].join();
    }
    delete[] this->thread_pool;
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    std::latch latch(num_total_tasks);

    for (int i = 0; i < num_total_tasks; i++) {
        std::unique_lock<std::mutex> lock(this->queue_mtx);
        this->q.push(Task(runnable, i, num_total_tasks, &latch));
        lock.unlock();
    }

    latch.wait();

    return;
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    this->thread_pool = new std::thread[num_threads];
    this->num_threads = num_threads;

    for (int i = 0; i < num_threads; i++) {
        this->thread_pool[i] = std::thread([this] {
            while (this->cont) {
                std::unique_lock<std::mutex> lock(this->queue_mtx);
                cv.wait(lock,[this] { return !(this->cont) || !this->q.empty(); });

                if (!(this->cont)) {
                    break;
                }
                Task task = this->q.front();
                this->q.pop();
                lock.unlock();
                
                task.runnable->runTask(task.i, task.num_total_tasks);
                task.latch->count_down();

            }
        });
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    {
        std::lock_guard<std::mutex> lock(this->queue_mtx);
        this->cont = false;
    }
    this->cv.notify_all();

    for (int i = 0; i < this->num_threads; i++) {
        this->thread_pool[i].join();
    }

    delete[] this->thread_pool;
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    std::latch latch(num_total_tasks);
    
    std::unique_lock<std::mutex> lock(this->queue_mtx);
    for (int i = 0; i < num_total_tasks; i++) {
        this->q.push(Task(runnable, i, num_total_tasks, &latch));
    }
    lock.unlock();
    this->cv.notify_one();
    latch.wait();

    return;
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
