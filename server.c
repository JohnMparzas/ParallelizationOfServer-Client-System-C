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
#include <stdio.h>
#include <sys/time.h>
#include <pthread.h>

#define MY_PORT                 6769
#define BUF_SIZE                1160
#define KEY_SIZE                 128
#define HASH_SIZE               1024
#define VALUE_SIZE              1024
#define MAX_PENDING_CONNECTIONS   10

#define QUEUE_SIZE 10
#define THREAD_NUMBER 10
//---prototypes

void ArraytoQueue();
void *consumers(void *arg);
void my_printer(int );

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
/////////--structures


typedef struct timeval Time;

typedef struct {
  int fdconnection;
  Time starttime;
}QueueRequest;

// Definition of the database.
KISSDB *db = NULL;
//////////////////////////////
//----variables

long total_waiting_time=0.0;
long total_service_time=0.0;
int completed_requests=0;
int fd_new;
int Tail=0;
int reader_count=0;
int writer_count=0;

Time tv_c;
Time tv_exit_queue;
Time tv_done;
//-------conds
pthread_cond_t non_full_queue =PTHREAD_COND_INITIALIZER;
pthread_cond_t non_empty_queue =PTHREAD_COND_INITIALIZER;

pthread_cond_t non_writer =PTHREAD_COND_INITIALIZER;
pthread_cond_t non_readers =PTHREAD_COND_INITIALIZER;

//-----arrays
pthread_t threads[THREAD_NUMBER];
QueueRequest queue[THREAD_NUMBER];


//----mutex

pthread_mutex_t queue_lock=PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t kissdb_lock=PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t kissdb_readers_lock=PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t done_mutex_time=PTHREAD_MUTEX_INITIALIZER;

/////////////////////////////////
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
  printf("%%%%%%%%%%%%%%%%%parse_request1%%%%%%%%%%%%%%%%%%%%%%%%%%\n");
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
    strncpy(req->key, token, KEY_SIZE);
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
  printf("%%%%%%%%%%%%%%%%%parse_request_end%%%%%%%%%%%%%%%%%%%%%%%%%%\n");
  return req;
}

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
    long done_sec;
    long done_usec;

    // Clean buffers.
    memset(response_str, 0, BUF_SIZE);
    memset(request_str, 0, BUF_SIZE);
    printf("%%%%%%%%%%%%%%%%%process_request1%%%%%%%%%%%%%%%%%%%%%%%%%\n");
    // receive message.
    numbytes = read_str_from_socket(socket_fd, request_str, BUF_SIZE);
    
    // parse the request.
    if (numbytes) {
      request = parse_request(request_str);
      if (request) {
        switch (request->operation) {
          case GET:
 	    printf("%%%%%%%%%%%%%%%%%GET_1%%%%%%%%%%%%%%%%%%%%%%%%%%\n");
            pthread_mutex_lock(&kissdb_lock);
	    while(writer_count!=0){
		pthread_cond_wait(&non_writer,&kissdb_lock);
	    }
	    reader_count++;

	    pthread_mutex_unlock(&kissdb_lock);
	    printf("%%%%%%%%%%%%%%%%%get_2%%%%%%%%%%%%%%%%%%%%%%%%%%\n");
            // Read the given key from the database.
            if (KISSDB_get(db, request->key, request->value))
              sprintf(response_str, "GET ERROR\n");
            else
              sprintf(response_str, "GET OK: %s\n", request->value);

	    pthread_mutex_lock(&kissdb_readers_lock);
	    reader_count--;
	    pthread_cond_signal(&non_readers);
	    pthread_mutex_unlock(&kissdb_readers_lock);
	    printf("%%%%%%%%%%%%%%%%%get_3%%%%%%%%%%%%%%%%%%%%%%%%%%\n");
            break;

          case PUT:
	    printf("%%%%%%%%%%%%%%%%%put_1%%%%%%%%%%%%%%%%%%%%%%%%%%\n");
	    pthread_mutex_lock(&kissdb_lock);
	    while(reader_count!=0){
		pthread_cond_wait(&non_readers,&kissdb_lock);
	    }
	    writer_count++;
		
	  
            // Write the given key/value pair to the database.
            if (KISSDB_put(db, request->key, request->value)){
              sprintf(response_str, "PUT ERROR\n");
               printf("@@@@@@@@@@@@@@@@@@@@@@@@@@@1\n");
            }else{
              sprintf(response_str, "PUT OK\n");
	       printf("@@@@@@@@@@@@@@@@@@@@@@@@@@@2\n");	
	    }
	    writer_count--;
	    pthread_cond_signal(&non_writer);
	    pthread_mutex_unlock(&kissdb_lock);
	    printf("%%%%%%%%%%%%%%%%%put_2%%%%%%%%%%%%%%%%%%%%%%%%%%\n");


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
    printf("%%%%%%%%%%%%%%%%%!!!!!!!!!!!%%%%%%%%%%%%%%%%%%%%%%%%%%");
}

/*
 * @name main - The main routine.
 *
 * @return 0 on success, 1 on error.
 */


void my_printer(int sig){
        pthread_mutex_lock(&done_mutex_time);
        
        printf("========================P========================\n");
	printf("completed_requests : %d \n",completed_requests);
	if(completed_requests==0){
		printf("average_waiting_time : 0.0\n");
		printf("average_service_time : 0.0\n");
	}else{
		
		printf("average_waiting_time : %f \n",(float) total_waiting_time/completed_requests);
		printf("average_service_time :%f \n",(float) total_service_time/completed_requests);
		
	}
	printf("===============================================\n");
	
        pthread_mutex_unlock(&done_mutex_time);

}

int main() {

  int socket_fd,              // listen on this socket for new connections
      new_fd;                 // use this socket to service a new connection
  socklen_t clen;
  struct sockaddr_in server_addr,  // my address information
                     client_addr;  // connector's address information

  
  signal(SIGTSTP,my_printer);  //signal
  // create socket
  if ((socket_fd = socket(AF_INET, SOCK_STREAM, 0)) == -1)
    ERROR("socket()");
    printf("%%%%%%%%%%%%%%%%%SOCKET%%%%%%%%%%%%%%%%%%%%%%%%%%\n");
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
  printf("%%%%%%%%%%%%%%%%%LISTENING%%%%%%%%%%%%%%%%%%%%%%%%%%\n");
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
  int i;
  for(i=0;i<THREAD_NUMBER;i++){
	pthread_create(&threads[i],NULL,consumers,(void *) i);
  }
  printf("%%%%%%%%%%%%%%%%%THREAADS_ON%%%%%%%%%%%%%%%%%%%%%%%%%%\n");
  

  // main loop: wait for new connection/requests
  while (1) { 
    // wait for incomming connection
    if ((new_fd = accept(socket_fd, (struct sockaddr *)&client_addr, &clen)) == -1) {
      ERROR("accept()");
    }

    printf("%%%%%%%%%%%%%%%%%ACCEPT_WHILE%%%%%%%%%%%%%%%%%%%%%%%%%%\n");
    QueueRequest queue_r;
    pthread_mutex_lock(&queue_lock);
    while(Tail==QUEUE_SIZE){
	pthread_cond_wait(&non_full_queue,&queue_lock);
    }

    gettimeofday(&tv_c,NULL);
    queue_r.starttime=tv_c;
    queue_r.fdconnection=new_fd;
    queue[Tail]=queue_r;
    Tail++;

    pthread_cond_signal(&non_empty_queue);

    pthread_mutex_unlock(&queue_lock);
     printf("%%%%%%%%%%%%%%%%%ADD REQUEST%%%%%%%%%%%%%%%%%%%%%%%%%%\n");
    
     

    // got connection, serve request
    fprintf(stderr, "(Info) main: Got connection from '%s'\n", inet_ntoa(client_addr.sin_addr));
    
    
  }
   for(i=0;i<THREAD_NUMBER;i++){
	pthread_join(threads[i],NULL);
  } 
   

  // Destroy the database.
  // Close the database.
  KISSDB_close(db);

  // Free memory.
  if (db)
    free(db);
  db = NULL;

  return 0; 
}

void *consumers(void *arg){
    long start_sec;
    long start_usec;
    long exit_sec;
    long exit_usec;
    long done_sec;
    long done_usec;
    Time exit_time;
    
    
    
    Time start_time;
    QueueRequest exiting_request;
    printf("%%%%%%%%%%%%%%%%%WELCOME_CONSUMERS%%%%%%%%%%%%%%%%%%%%%%%%%%\n");
    while(1){
      pthread_mutex_lock(&queue_lock);
   
      while(Tail==0){
	  pthread_cond_wait(&non_empty_queue,&queue_lock);
      }
      
      exiting_request=queue[0];
      ArraytoQueue();
      Tail--;
      gettimeofday(&tv_exit_queue,NULL);

     
      exit_sec=tv_exit_queue.tv_sec;
      exit_usec=tv_exit_queue.tv_usec;

      start_time=exiting_request.starttime;
      start_sec=start_time.tv_sec;
      start_usec=start_time.tv_usec;
      total_waiting_time=total_waiting_time+1000000*(exit_sec-start_sec)+(exit_usec-start_usec);

      fd_new=exiting_request.fdconnection;
    

      pthread_cond_signal(&non_full_queue);

      pthread_mutex_unlock(&queue_lock);
      process_request(fd_new);
      pthread_mutex_lock(&done_mutex_time);
       
       completed_requests++;
       gettimeofday(&tv_done,NULL);
       done_sec=tv_done.tv_sec;
       done_usec=tv_done.tv_usec;
       total_service_time=total_service_time+1000000*(done_sec-exit_sec)+(done_usec-exit_usec);
       pthread_mutex_unlock(&done_mutex_time);
      printf("%%%%%%%%%%%%%%%%%process_request_AFTER%%%%%%%%%%%%%%%%%%%%%%%%%%\n");
      
      printf("%%%%%%%%%%%%%%%%%CLOSE_FD %%%%%%%%%%%%%%%%%%%%%%%%%%\n");
      close(fd_new);
  }
    
    
}

void ArraytoQueue(){
	int i;
	for(i=0;i<Tail-1;i++){
		queue[i]=queue[i+1];
	}

}



