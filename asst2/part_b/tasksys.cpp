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
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
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
    // Runs launches one at a time in submission order, which always satisfies deps.
    run(runnable, num_total_tasks);
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

SpinTask::SpinTask(IRunnable* runnable, int i, int num_total_tasks, std::latch* latch) {
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
                if (this->q_size == 0) {
                    std::this_thread::yield();
                    continue;
                }
                std::unique_lock<std::mutex> lock(this->queue_mtx);
                if (!this->q.empty()) {
                    SpinTask task = this->q.front();
                    this->q.pop();
                    this->q_size.fetch_sub(1);
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

    std::unique_lock<std::mutex> lock(this->queue_mtx);
    for (int i = 0; i < num_total_tasks; i++) {
        this->q.push(SpinTask(runnable, i, num_total_tasks, &latch));
    }
    this->q_size.store(this->q.size());
    lock.unlock();

    while (!latch.try_wait()) {}

    return;
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // Runs launches one at a time in submission order, which always satisfies deps.
    run(runnable, num_total_tasks);
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

Task::Task(IRunnable *runnable, int i, int num_total_tasks, int launch_id, std::atomic<int> *remaining)
{
    this->runnable = runnable;
    this->i = i;
    this->num_total_tasks = num_total_tasks;
    this->launch_id = launch_id;
    this->remaining = remaining;
}

Launch::Launch(std::vector<Task> tasks, int launch_id, std::vector<TaskID> deps, std::vector<TaskID> waiting, std::atomic<int> *remaining, std::function<void()> on_complete)
{
    this->tasks = tasks;
    this->deps = deps;
    this->waiting = waiting;
    this->remaining = remaining;
    this->launch_id = launch_id;
    this->on_complete = on_complete;
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

    for (int i = 0; i < num_threads; i++)
    {
        this->thread_pool[i] = std::thread([this]
                                           {
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

                if (task.remaining->fetch_sub(1) == 1) {
                    int launch_id = task.launch_id;
                    std::unique_lock<std::mutex> lock(this->current_mtx);
                    auto it = this->current.find(launch_id);
                    if (it != this->current.end()) {
                        Launch launch = this->current[launch_id];
                        lock.unlock();
                        launch.on_complete();  
                    }
                }
            } });
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    {
        std::lock_guard<std::mutex> lock(this->queue_mtx);
        this->cont = false;
    }
    this->cv.notify_all();

    for (int i = 0; i < this->num_threads; i++)
    {
        this->thread_pool[i].join();
    }

    for (auto &pair : this->current)
    {
        delete pair.second.remaining;
    }
    for (auto &pair : this->waiting)
    {
        delete pair.second.remaining;
    }

    delete[] this->thread_pool;
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable *runnable, int num_total_tasks)
{
    runAsyncWithDeps(runnable, num_total_tasks, std::vector<TaskID>());
    sync();
    return;
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps)
{
    int launchId = this->prev_launch_id.fetch_add(1) + 1;

    std::vector<Task> tasks;
    void *remaining = new std::atomic<int>(num_total_tasks);
    for (int i = 0; i < num_total_tasks; i++)
    {
        tasks.emplace_back(runnable, i, num_total_tasks, launchId, static_cast<std::atomic<int> *>(remaining));
    }

    std::vector<TaskID> pending_deps;
    std::unique_lock<std::mutex> curr_lock(this->current_mtx);
    std::unique_lock<std::mutex> wait_lock(this->waiting_mtx);
    for (auto &dep : deps)
    {
        auto it = this->current.find(dep);
        if (it != this->current.end())
        {
            pending_deps.push_back(dep);
            this->current[dep].waiting.push_back(launchId);
        }
        else
        {
            auto it2 = this->waiting.find(dep);
            if (it2 != this->waiting.end())
            {
                pending_deps.push_back(dep);
                this->waiting[dep].waiting.push_back(launchId);
            }
        }
    }

    auto upon_complete = [this, launchId]()
    {
        std::unique_lock<std::mutex> lock(this->current_mtx);
        Launch launch = this->current[launchId];

        for (auto &waiting_launch_id : launch.waiting)
        {
            std::unique_lock<std::mutex> lock(this->waiting_mtx);
            auto it = this->waiting.find(waiting_launch_id);
            if (it != this->waiting.end())
            {
                this->waiting[waiting_launch_id].deps.erase(std::remove(this->waiting[waiting_launch_id].deps.begin(), this->waiting[waiting_launch_id].deps.end(), launchId), this->waiting[waiting_launch_id].deps.end());
                if (this->waiting[waiting_launch_id].deps.empty())
                {
                    Launch ready_launch = this->waiting[waiting_launch_id];
                    this->waiting.erase(waiting_launch_id);

                    std::unique_lock<std::mutex> lock(this->queue_mtx);
                    for (auto &task : ready_launch.tasks)
                    {
                        this->q.push(task);
                    }

                    this->current[waiting_launch_id] = ready_launch;

                    lock.unlock();
                    this->cv.notify_all();
                }
            }
        }

        delete launch.remaining;
        this->current.erase(launchId);

        lock.unlock();
    };

    Launch launch(tasks, launchId, pending_deps, std::vector<TaskID>(), static_cast<std::atomic<int> *>(remaining), upon_complete);

    if (pending_deps.empty())
    {
        this->current[launchId] = launch;

        std::unique_lock<std::mutex> q_lock(this->queue_mtx);
        for (const auto &task : launch.tasks)
        {
            this->q.push(task);
        }
        q_lock.unlock();

        this->cv.notify_all();
    }
    else
    {
        this->waiting[launchId] = launch;
    }
    curr_lock.unlock();
    wait_lock.unlock();

    return launchId;
}

void TaskSystemParallelThreadPoolSleeping::sync()
{
    while (true)
    {
        std::unique_lock<std::mutex> lock(this->current_mtx);
        if (this->current.empty())
        {
            lock.unlock();
            break;
        }
        lock.unlock();
        std::this_thread::yield();
    }

    while (true)
    {
        std::unique_lock<std::mutex> lock(this->waiting_mtx);
        if (this->waiting.empty())
        {
            lock.unlock();
            break;
        }
        lock.unlock();
        std::this_thread::yield();
    }
}
