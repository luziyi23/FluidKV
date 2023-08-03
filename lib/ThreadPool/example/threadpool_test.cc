#include <iostream>
#include <unistd.h>
#include <atomic>

#include "threadpool.h"
#include "threadpool_imp.h"

#define THREAD_COUNT 1

using std::cout;
using std::endl;

struct Cxt
{
    int thread_id;
    std::atomic<int> *last_id;

    Cxt(std::atomic<int> *p, int i) : thread_id(i), last_id(p) {}
};

void Thread1(void *ptr)
{
    printf("T1 init\n");
    Cxt *cxt = reinterpret_cast<Cxt *>(ptr);
    int count = THREAD_COUNT;
    while (count--)
    {
        printf("-------T1 *%d* running ------- \n", cxt->thread_id);
        sleep(1);
    }
}

void Thread2(void *ptr)
{
    printf("T2 init\n");
    Cxt *cxt = reinterpret_cast<Cxt *>(ptr);
    int count = THREAD_COUNT;
    while (count--)
    {
        printf("-------T2 *%d* running ------- \n", cxt->thread_id);
        sleep(1);
    }
}

void finish1(void *ptr)
{
    Cxt *cxt = reinterpret_cast<Cxt *>(ptr);
    printf("Finish excute %d\n", cxt->thread_id);
    delete cxt;
}

int main(int argc, char *argv[])
{
    std::atomic<int> last_id(0);

    auto *thread_pool1 = new ThreadPoolImpl();
    auto *thread_pool2 = new ThreadPoolImpl();

    // Set the background threads in threadpool
    // threadpool1 have 3 threads and with lower IO priority
    // threadpool2 have 7 threads and with lower CPU priority
    thread_pool1->SetBackgroundThreads(2);
    thread_pool2->SetBackgroundThreads(1);

    thread_pool1->LowerIOPriority();
    thread_pool2->LowerCPUPriority();
    Cxt *cxt[10];
    for (int i = 0; i < 10;i++)
    {
        Cxt *cxt_i = new Cxt(&last_id, i);
        cxt[i] = cxt_i;
        if (i % 2 == 0)
        {
            printf("T1 schedule\n");
            thread_pool1->Schedule(&Thread1, cxt_i, cxt_i, &finish1);
        }
        else
        {
            printf("T2 schedule\n");
            thread_pool2->Schedule(&Thread2, cxt_i, cxt_i, &finish1);
        }
    }
    // thread_pool1->UnSchedule(cxt[6]);
    // thread_pool2->UnSchedule(cxt_jj[9]);
    // sleep(7);
    std::cout << thread_pool1->GetQueueLen() <<std::endl;
    std::cout << thread_pool2->GetQueueLen() <<std::endl;
        // for(int i = 0; i < 10;i++)
        // delete cxt[i];
    thread_pool1->WaitForJobsAndJoinAllThreads();
    thread_pool2->WaitForJobsAndJoinAllThreads();
    
    // thread_pool1->JoinAllThreads(); // this function drop all the jobs in queue
    // thread_pool2->JoinAllThreads();

    return 0;
}
