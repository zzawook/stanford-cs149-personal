#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <thread>
#include <queue>
#include <mutex>
#include <latch>
#include <atomic>
#include <condition_variable>
#include <unordered_map>
#include <functional>

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
    private:
        std::thread* threads;
        int num_threads;
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class SpinTask {
    public:
        IRunnable* runnable;
        int i;
        int num_total_tasks;
        std::latch* latch;
        SpinTask(IRunnable* runnable, int i, int num_total_tasks, std::latch* latch);
};

class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
        int num_threads;

    private:
        std::queue<SpinTask> q;
        std::mutex queue_mtx;
        std::thread* thread_pool;
        std::atomic<bool> cont{true};
        std::atomic<int> q_size;
};
class Task
{
public:
    IRunnable *runnable;
    int i;
    int num_total_tasks;
    std::atomic<int> *remaining;
    int launch_id;
    Task(IRunnable *runnable, int i, int num_total_tasks, int launch_id, std::atomic<int> *remaining);
};

class Launch
{
public:
    std::vector<Task> tasks;
    std::vector<TaskID> deps;
    std::vector<TaskID> waiting;
    std::atomic<int> *remaining{0};
    std::function<void()> on_complete;
    int launch_id;
    Launch(std::vector<Task> tasks, int launch_id, std::vector<TaskID> deps, std::vector<TaskID> waiting, std::atomic<int> *remaining, std::function<void()> on_complete = []() {});
    Launch() = default;
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
        int num_threads;

    private:
        std::atomic<bool> cont{true};
        std::condition_variable cv;

        std::thread *thread_pool;

        std::queue<Task> q;
        std::mutex queue_mtx;

        std::unordered_map<int, Launch> current;
        std::mutex current_mtx;
        std::unordered_map<int, Launch> waiting;
        std::mutex waiting_mtx;

        std::atomic<int> prev_launch_id{0};
};

#endif
