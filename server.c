/* server.c

   Sample code of 
   Assignment L1: Simple multi-threaded key-value server
   for the course MYY601 Operating Systems, University of Ioannina 

   (c) S. Anastasiadis, G. Kappes 2016

*/


#include <signal.h>
#include <sys/stat.h>
#include "utils.h"
#include "kissdb.h"

#include <time.h>
#include <pthread.h>
#include <stdio.h>

#define MY_PORT                 6767
#define BUF_SIZE                1160
#define KEY_SIZE                 128
#define HASH_SIZE               1024
#define VALUE_SIZE              1024
#define MAX_PENDING_CONNECTIONS   10

#define QUEUE_SIZE 20
#define CONSUMERS 10



// mutex declarations
pthread_mutex_t grabRequest = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mFullness = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t addTime = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mWriter = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t isFull = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t isEmpty = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t operate = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t elim = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t terminated = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mRemaining = PTHREAD_MUTEX_INITIALIZER;

// condition declarations
pthread_cond_t cIsFull = PTHREAD_COND_INITIALIZER;
pthread_cond_t cIsEmpty = PTHREAD_COND_INITIALIZER;
pthread_cond_t cWriter = PTHREAD_COND_INITIALIZER;
pthread_cond_t cTerminated = PTHREAD_COND_INITIALIZER;

// list of all the consumer threads
pthread_t ids[CONSUMERS];  

// Definition of the operation type.
typedef enum operation {
  PUT,
  GET
} Operation; 

// Definition of the request.
typedef struct request {
  Operation operation;
  char key[KEY_SIZE];  
  char value[VALUE_SIZE];
} Request;

// Definition of the database.
KISSDB *db = NULL;

/**
 * @name parse_request - Parses a received message and generates a new request.
 * @param buffer: A pointer to the received message.
 *
 * @return Initialized request on Success. NULL on Error.
 */
Request *parse_request(char *buffer) {
  char *token = NULL;
  Request *req = NULL;
  
  // Check arguments.
  if (!buffer)
    return NULL;
  
  // Prepare the request.
  req = (Request *) malloc(sizeof(Request));
  memset(req->key, 0, KEY_SIZE);
  memset(req->value, 0, VALUE_SIZE);

  // Extract the operation type.
  token = strtok(buffer, ":");    
  if (!strcmp(token, "PUT")) {
    req->operation = PUT;
  } else if (!strcmp(token, "GET")) {
    req->operation = GET;
  } else {
    free(req);
    return NULL;
  }
  
  // Extract the key.
  token = strtok(NULL, ":");
  if (token) {

      //was strcpy
    memcpy(req->key, token, KEY_SIZE);
  } else {
    free(req);
    return NULL;
  }
  
  // Extract the value.
  token = strtok(NULL, ":");
  if (token) {
    strncpy(req->value, token, VALUE_SIZE);
  } else if (req->operation == PUT) {
    free(req);
    return NULL;
  }
  return req;
}

// global counters to count how many operations are currently being done to the list
int reader = 0;
int writer = 0;

/*
 * @name process_request - Process a client request.
 * @param socket_fd: The accept descriptor.
 *
 * @return
 */
void process_request(const int socket_fd) {
  char response_str[BUF_SIZE], request_str[BUF_SIZE];
    int numbytes = 0;
    Request *request = NULL;

    // Clean buffers.
    memset(response_str, 0, BUF_SIZE);
    memset(request_str, 0, BUF_SIZE);
    
    // receive message.
    numbytes = read_str_from_socket(socket_fd, request_str, BUF_SIZE);
    
    // parse the request.
    if (numbytes) {
      request = parse_request(request_str);
      if (request) {
          // HERE WE MANAGE THE GET PUT MUTEX
        switch (request->operation) {
          case GET:
            pthread_mutex_lock(&mWriter);
            while(writer != 0){
              pthread_cond_wait(&cWriter, &mWriter);
            
            }
            reader ++;
            pthread_mutex_unlock(&mWriter);
            
              
            // Read the given key from the database.
            if (KISSDB_get(db, request->key, request->value))
              sprintf(response_str, "GET ERROR\n");
            else
              sprintf(response_str, "GET OK: %s\n", request->value);

            pthread_mutex_lock(&mWriter);
            reader --;
              if(reader == 0){
                pthread_cond_broadcast(&cWriter);
              } 
            pthread_mutex_unlock(&mWriter);
            break;
          case PUT:
            
            pthread_mutex_lock(&mWriter);
            while(writer != 0 || reader != 0){
              pthread_cond_wait(&cWriter, &mWriter);
            
            }
            writer = 1;
            pthread_mutex_unlock(&mWriter);
            

            // Write the given key/value pair to the database.
            if (KISSDB_put(db, request->key, request->value)) 
              sprintf(response_str, "PUT ERROR\n");
            else
              sprintf(response_str, "PUT OK\n");

            pthread_mutex_lock(&mWriter);
            writer = 0;
            pthread_cond_broadcast(&cWriter);
            pthread_mutex_unlock(&mWriter);

            break;
          default:
            // Unsupported operation.
            sprintf(response_str, "UNKOWN OPERATION\n");
        }
        

        // Reply to the client.
        write_str_to_socket(socket_fd, response_str, strlen(response_str));
        if (request)
          free(request);
        request = NULL;
        return;
      }
    }
    // Send an Error reply to the client.
    sprintf(response_str, "FORMAT ERROR\n");
    write_str_to_socket(socket_fd, response_str, strlen(response_str));
}

struct customer{

    long recieveTime;
    long startTime;
    int socketFD;
    //struct request request;

}customer;

// consumer thread stopped counter
int killed = 0;

// variable to signal the consumers to stop
int eliminate = 0;

// variable to signal producer to stop accepting new requests
int contract = 0;

// global time variables
long total_wainting_time = 0;
long total_service_time = 0;
int completed_requests = 0;


// Global Queue declarations
int head = 0;
int tail = 0;
int fullness = 0;

struct customer queue[QUEUE_SIZE];



// Time function
long getTime(){

    long time;
    struct timespec dur;
    struct timespec *duration = &dur;
    clock_gettime(CLOCK_MONOTONIC, duration);
    time = dur.tv_nsec;
    time += (dur.tv_sec * 1000000000);
    return time;
}

// stats function
int statistics(){

  long waitingMO = total_wainting_time / completed_requests;
  long processMO = total_service_time / completed_requests;

  printf("completed requests: %i.\n", completed_requests);
  printf("average waiting time: '%ld' nsec.\n", waitingMO);
  printf("average service time: '%ld' nsec.\n", processMO);
  return 0;
}



// terminal stop signal handle
void stpHandle(int sig){
  printf("\nTerminal stop signal caught!\nPlease hold while the last requests are taken care of.\n");
  // signal producer to stop after completing current request
  contract = 1;
  return 0;
}



//////////////////////
// consumer routine //
//////////////////////
void *consumer(){
  //printf("i m a thread\n");
  int id;
  for(int i = 0; i < CONSUMERS; i ++){
    if(pthread_equal(pthread_self(), ids[i]) == 1){
      id = i;
      //printf("consumer thread no: %i started.\n", i);
      break;
    }

  }
  while(1){
    // empty queue check and wait
    pthread_mutex_lock(&isEmpty);
    while(fullness == 0 && eliminate == 0){
      pthread_cond_wait(&cIsEmpty, &isEmpty);
    }
    pthread_mutex_unlock(&isEmpty);

    if(eliminate == 1){
        pthread_mutex_lock(&mRemaining);        
        // check if there are any remaining requests
        if(fullness == 0){
          pthread_mutex_unlock(&mRemaining);

          pthread_mutex_lock(&terminated);
          killed ++;
          if(killed == CONSUMERS){
            pthread_cond_signal(&cTerminated);
          }
          pthread_mutex_unlock(&terminated);
          return 0;
        }
        pthread_mutex_unlock(&mRemaining);
    }
    int wasFull;

    pthread_mutex_lock(&grabRequest);
    // struct customer *customer; // = queue[head];
    struct customer cust = queue[head];
    cust.startTime = getTime();
    if( head == (QUEUE_SIZE - 1) ){
        head = 0;
    } else {
        head ++;
    }
    pthread_mutex_unlock(&grabRequest);

    pthread_mutex_lock(&mFullness);
    fullness --;
    if(fullness + 1 == QUEUE_SIZE){
         pthread_cond_signal(&cIsFull);
    }
    pthread_mutex_unlock(&mFullness);

    process_request(cust.socketFD);

    long finishTime = getTime();

    pthread_mutex_lock(&addTime);
    total_wainting_time = cust.startTime - cust.recieveTime;
    long total_service_time = finishTime - cust.startTime;
    completed_requests ++;
    pthread_mutex_unlock(&addTime);
    
    //printf("process time: %li", processTime);
    close(cust.socketFD);

    if(eliminate == 1){
      pthread_mutex_lock(&elim);
      killed ++;
      if(killed == CONSUMERS){
        pthread_cond_signal(&cTerminated);
      pthread_mutex_unlock(&elim);
      return 0;
      }
    }
  }
    return 0;
}

/*
 * @name main - The main routine.
 *
 * @return 0 on success, 1 on error.
 */
int main() {
  signal(SIGTSTP, stpHandle);
  //printf("hiiiiiiiiii\n");

  for(int i = 0; i < CONSUMERS; i ++){ 
    //printf("creating thread '%i'\n", i);
    pthread_create(&ids[i], NULL, consumer, NULL); // look into second NULL

  }

  int socket_fd,              // listen on this socket for new connections
      new_fd;                 // use this socket to service a new connection
  socklen_t clen;
  struct sockaddr_in server_addr,  // my address information
                     client_addr;  // connector's address information

  // create socket
  if ((socket_fd = socket(AF_INET, SOCK_STREAM, 0)) == -1)
    ERROR("socket()");

  // Ignore the SIGPIPE signal in order to not crash when a
  // client closes the connection unexpectedly.
  signal(SIGPIPE, SIG_IGN);
  
  // create socket adress of server (type, IP-adress and port number)
  bzero(&server_addr, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = htonl(INADDR_ANY);    // any local interface
  server_addr.sin_port = htons(MY_PORT);
  
  // bind socket to address
  if (bind(socket_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) == -1)
    ERROR("bind()");
  
  // start listening to socket for incomming connections
  listen(socket_fd, MAX_PENDING_CONNECTIONS);
  fprintf(stderr, "(Info) main: Listening for new connections on port %d ...\n", MY_PORT);
  clen = sizeof(client_addr);

  // Allocate memory for the database.
  if (!(db = (KISSDB *)malloc(sizeof(KISSDB)))) {
    fprintf(stderr, "(Error) main: Cannot allocate memory for the database.\n");
    return 1;
  }
  
  // Open the database.
  if (KISSDB_open(db, "mydb.db", KISSDB_OPEN_MODE_RWCREAT, HASH_SIZE, KEY_SIZE, VALUE_SIZE)) {
    fprintf(stderr, "(Error) main: Cannot open the database.\n");
    return 1;
  }

  struct customer cust;
  // here we should create the consumer threads

  
  

  // main loop: wait for new connection/requests
  while (contract == 0) { 
    // wait for incomming connection
    if ((new_fd = accept(socket_fd, (struct sockaddr *)&client_addr, &clen)) == -1) {
      ERROR("accept()");
    }
    
    // got connection, serve request
    fprintf(stderr, "(Info) main: Got connection from '%s'\n", inet_ntoa(client_addr.sin_addr));
    
    ////////////////////////////////////////////////////////////

    // full queue check and wait
    pthread_mutex_lock(&isFull);
    if(fullness == QUEUE_SIZE - 1){
      pthread_cond_wait(&cIsFull, &isFull);
    }
    pthread_mutex_unlock(&isFull);
    //printf("fullness: %i\n", fullness);
    int wasEmpty;
    // pthread_t tid;
    cust.recieveTime = getTime();
    cust.socketFD = new_fd;

    // put new request in queue
    queue[tail] = cust;
    if(tail == (QUEUE_SIZE - 1) ){
      tail = 0;
    } else{
      tail ++;
    }
    wasEmpty = fullness;
    
    pthread_mutex_lock(&mFullness);
    fullness ++;
    pthread_mutex_unlock(&mFullness);

    if(wasEmpty == 0){
      // signal not empty
      pthread_cond_signal(&cIsEmpty);
    }

    //pthread_create(&tid, NULL, consumer, (void *) &cust);
    //process_request(new_fd);
    //close(new_fd);
  }  
  printf("Stopped new requests");
  // here i go killing again
  eliminate = 1;

  // wake up any sleeping consumers
  pthread_cond_broadcast(&cIsEmpty);

  // wait for all the consumers to stop
  pthread_mutex_lock(&terminated);
  while(killed < CONSUMERS){
    pthread_cond_wait(&cTerminated, &terminated);
  }
  pthread_mutex_unlock(&terminated);

  // Destroy the database.
  // Close the database.
  KISSDB_close(db);

  // Free memory.
  if (db)
    free(db);
  db = NULL;

  return 0; 
}

