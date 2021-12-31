#include <mpi.h>
#include <unistd.h>
#include <stdlib.h>
#include <pthread.h>


enum TAG
{
    TOKEN,
    ACK
};

struct _token_thread_data {
    int token_version;
    int node;
    int next_node;
    int wait_for_ack_time;
};

pthread_mutex_t received_mutex = PTHREAD_MUTEX_INITIALIZER, token_owner_mutex = PTHREAD_MUTEX_INITIALIZER;
bool received = false, token_owner = false;
int send_loss_threshold;

bool send_message(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm) {
    int probability = (rand() % 100);
    if (tag == TOKEN) {
        // modify probability for TOKEN messages
        probability = 100;
    }
    if (tag == ACK) {
        // modify probability for TOKEN messages
    }
    if (probability >= send_loss_threshold) {
        MPI_Send(buf, count, datatype, dest, tag, comm);
    }
    return probability >= send_loss_threshold;
}

void send_token(const int token_buf, int dest, int rank) {
    bool success = send_message(&token_buf, 1, MPI_INT, dest, TOKEN, MPI_COMM_WORLD);
    printf("\033[0;33m%02d  Sending message TOKEN(version=%d) to %02d%s\033[0m\n", rank, token_buf, dest, success ? "" : " - DROPPED");
}

void resend_token(const int token_buf, int dest, int rank, int resend_counter) {
    bool success = send_message(&token_buf, 1, MPI_INT, dest, TOKEN, MPI_COMM_WORLD);
    printf("\033[0;33m%02d  Resending(%d) message TOKEN(version=%d) to %02d%s\033[0m\n", rank, resend_counter, token_buf, dest, success ? "" : " - DROPPED");
}

void send_ack(const int *ack_buf, int dest, int rank) {
    bool success = send_message(ack_buf, 2, MPI_INT, dest, ACK, MPI_COMM_WORLD);
    printf("%02d  Sending message ACK(to=%d,version=%d) to %02d%s\n", rank, ack_buf[0], ack_buf[1], dest, success ? "" : " - DROPPED");
}

void * token_processing(void *_data) {
    struct _token_thread_data *data =  (struct _token_thread_data*) _data;

    int critical_section_time = rand() % 3 + 2;
    printf("\033[0;31m%02d  Entering critical section for %d seconds\033[0m\n", data->node, critical_section_time);
    sleep(critical_section_time);
    printf("\033[0;31m%02d  Exiting critical section\033[0m\n", data->node);

    // clear 'received' flag
    pthread_mutex_lock(&received_mutex);
    received = false;
    int resend = 0;

    while (!received) {
        pthread_mutex_unlock(&received_mutex);
        if (resend == 0) {
            pthread_mutex_lock(&token_owner_mutex);
            token_owner = false;
            pthread_mutex_unlock(&token_owner_mutex);
            send_token(data->token_version, data->next_node, data->node);
        } else {
            resend_token(data->token_version, data->next_node, data->node, resend);
        }
        resend++;
        
        // wait for ack given time (size * message_timeout)
        usleep(data->wait_for_ack_time);

        // check if received ack for token inside mutex
        pthread_mutex_lock(&received_mutex);
    }
    pthread_mutex_unlock(&received_mutex);
    delete data;
    pthread_exit(0);
}

void init_token_thread(pthread_t* thread, int token_version, int rank, int next_node, int size, int message_timeout) {
    struct _token_thread_data *token_data = new struct _token_thread_data;
    token_data->token_version = token_version;
    token_data->node = rank;
    token_data->next_node = next_node;
    int wait_time = size * message_timeout;
    token_data->wait_for_ack_time = wait_time;
    pthread_create(thread, NULL, &token_processing, (void*) token_data);
}

int main(int argc, char **argv)
{
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    if (provided < MPI_THREAD_MULTIPLE)
    {
        printf("ERROR: The MPI library does not have full thread support\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    int size, rank;
    MPI_Status status;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    srand(time(NULL) + rank);
    int previous_node = (rank + size - 1) % size;
    int next_node = (rank + 1) % size;
    int token_version = rank == 0 ? 0 : 1;
    int token_buffer, ack_buffer[2]; // first value - receiver, second value - acknowledged token version
    int message_type_index;
    bool first_round = true;
    int message_timeout = atoi(argv[1]) * 1000; // milisekundy - mikrosekundy
    send_loss_threshold = atoi(argv[2]); // probability threshold for sending messages
    pthread_t token_thread;

    if (rank == 0) {
        first_round = false;
        token_owner = true;
        init_token_thread(&token_thread, token_version, rank, next_node, size, message_timeout);
    }

    // requests[0] for TOKEN, requests[1] ACK
    MPI_Request requests[2];
    MPI_Irecv(&token_buffer, 1, MPI_INT, previous_node, TOKEN, MPI_COMM_WORLD, &requests[0]);
    MPI_Irecv(ack_buffer, 2, MPI_INT, previous_node, ACK, MPI_COMM_WORLD, &requests[1]);

    while (true)
    {
        
        MPI_Waitany(2, requests, &message_type_index, &status);

        if (message_type_index == 0) {
            // received TOKEN message
            printf("\033[0;33m%02d  Received message TOKEN(version=%d) from %02d\033[0m\n", rank, token_buffer, status.MPI_SOURCE);

            if ((rank == 0 && token_buffer == token_version) || (rank != 0 && token_buffer != token_version)) {
                // first TOKEN message in the round, not retransmission
                pthread_mutex_lock(&received_mutex);
                received = true;
                pthread_mutex_unlock(&received_mutex);
                pthread_mutex_lock(&token_owner_mutex);
                token_owner = true;
                pthread_mutex_unlock(&token_owner_mutex);

                // update token_version
                if (rank == 0) {
                    token_version = token_buffer == 0 ? 1 : 0;
                } else {
                    token_version = token_buffer;
                }

                // start critical section thread
                if (first_round) {
                    first_round = false;
                } else {
                    pthread_join(token_thread, NULL);
                }
                init_token_thread(&token_thread, token_version, rank, next_node, size, message_timeout);
            }

            // Node send ACK in response to TOKEN iff it is still in possesion of token.
            // After sending TOKEN message itself, node ignore retransmitted TOKEN messages from previous node.
            pthread_mutex_lock(&token_owner_mutex);
            bool respond = token_owner;
            pthread_mutex_unlock(&token_owner_mutex);

            if (respond) {
                ack_buffer[0] = previous_node;
                ack_buffer[1] = token_buffer;
                send_ack(ack_buffer, next_node, rank);
            }
            
            MPI_Irecv(&token_buffer, 1, MPI_INT, previous_node, TOKEN, MPI_COMM_WORLD, &requests[0]);

        } else {
            // received ACK message
            if (ack_buffer[0] == rank) {
                // message to us
                printf("\033[0;32m%02d  Received message ACK(to=%d, version=%d) from %02d\033[0m\n", rank, ack_buffer[0], ack_buffer[1], status.MPI_SOURCE);
                if (ack_buffer[1] == token_version) {
                    // ack for latest TOKEN message
                    pthread_mutex_lock(&received_mutex);
                    received = true;
                    pthread_mutex_unlock(&received_mutex);
                } else {
                    printf("ERROR!! Received ACK for own TOKEN message with wrong version!\n");
                }
            } else {
                // message to someone else

                // After receiving TOKEN message, it is guaranteed that we does not receive any ACK message "from behind" - previous node won't send it.
                // Any ACK message received can be treated as ACK to self (it is to self or someone "in front").
                // We still need token_version to distinguish between retransmitted 'old' token and first 'new' token.

                printf("%02d  Received forward message ACK(to=%d, version=%d) from %02d\n", rank, ack_buffer[0], ack_buffer[1], status.MPI_SOURCE);

                pthread_mutex_lock(&received_mutex);
                received = true;
                pthread_mutex_unlock(&received_mutex);

                send_ack(ack_buffer, next_node, rank);
            }
            MPI_Irecv(ack_buffer, 2, MPI_INT, previous_node, ACK, MPI_COMM_WORLD, &requests[1]);
        }
    }

    MPI_Finalize();
}