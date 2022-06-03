/* client.c

   Sample code of 
   Assignment L1: Simple multi-threaded key-value server
   for the course MYY601 Operating Systems, University of Ioannina 

   (c) S. Anastasiadis, G. Kappes 2016

*/


#include "utils.h"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define SERVER_PORT     6769
#define BUF_SIZE        2048
#define MAXHOSTNAMELEN  1024
#define MAX_STATION_ID   128
#define ITER_COUNT          1
#define GET_MODE           1
#define PUT_MODE           2
#define USER_MODE          3
#define MULTI_SENDER      4
#define NUM_OF_THREADS    16

/**
 * @name print_usage - Prints usage information.
 * @return
 */

pthread_mutex_t requests_lock=PTHREAD_MUTEX_INITIALIZER;
//pthread_t threads[NUM_OF_THREADS];
int request_number;
char  * buffer[NUM_OF_THREADS];


int socket_fds[NUM_OF_THREADS];

struct sockaddr_in server_addr;
typedef struct thread_args{
	struct sockaddr_in server_addr;
	char *buffer;
}arg_thread;

void print_usage() {
  fprintf(stderr, "Usage: client [OPTION]...\n\n");
  fprintf(stderr, "Available Options:\n");
  fprintf(stderr, "-h:             Print this help message.\n");
  fprintf(stderr, "-a <address>:   Specify the server address or hostname.\n");
  fprintf(stderr, "-o <operation>: Send a single operation to the server.\n");
  fprintf(stderr, "                <operation>:\n");
  fprintf(stderr, "                PUT:key:value\n");
  fprintf(stderr, "                GET:key\n");
  fprintf(stderr, "-i <count>:     Specify the number of iterations.\n");
  fprintf(stderr, "-g:             Repeatedly send GET operations.\n");
  fprintf(stderr, "-p:             Repeatedly send PUT operations.\n");
}

/**
 * @name talk - Sends a message to the server and prints the response.
 * @server_addr: The server address.
 * @buffer: A buffer that contains a message for the server.
 *const struct sockaddr_in !!!!!!!!!!!!!!!
 * @return
 */



	
void multi_task(){
        
        
	
  char rcv_buffer[BUF_SIZE];
  int socket_fd, numbytes;
  int request_number;
  
  buffer[0]="PUT:station.125:6";
  buffer[1]="GET:station.125";
  buffer[2]="PUT:station.120:3";
  buffer[3]="GET:station.121";
  buffer[4]="PUT:station.122:6";
  buffer[5]="GET:station.122";
  buffer[6]="PUT:station.119:2";
  buffer[7]="GET:station.118";
  buffer[8]="GET:station.112";
  buffer[9]="GET:station.111";
  buffer[10]="PUT:station.110:6";
  buffer[11]="GET:station.111";
  buffer[12]="PUT:station.123:6";
  buffer[13]="PUT:station.123:3";
  buffer[14]="GET:station.123";
  buffer[15]="GET:station.124";
  request_number=0;
  int pid=fork();
  if( pid==0){
     request_number++;
  }
  pid=fork();
   if(pid==0){
	request_number++;
  }
  pid=fork();
   if(pid==0){
	request_number++;
   }
  pid=fork();
   if(pid==0){
	request_number++;
   }
   //pthread_mutex_lock(&requests_lock);
  //char *buffer1="PUT:stations.125:6";  
  // create socket
  //myrandomrequest=randombytes_uniform(16);
  if ((socket_fd = socket(PF_INET, SOCK_STREAM, 0)) == -1) {
    ERROR("socket()");
  }

  // connect to the server.
  if (connect(socket_fd, (struct sockaddr*) &server_addr, sizeof(server_addr)) == -1) {
    ERROR("connect()");
  }
  
  // send message.
  //request_number=0;//rand()%15;
  //fprintf(stderr,"%d\n",request_number);
  //buffer2=buffer[request_number];
 
  //request_number = rand() % 16;
 
  fprintf(stderr,"-###@- %d\n",request_number);
  
  
  write_str_to_socket(socket_fd, buffer[request_number], strlen(buffer[request_number]));
   
   fprintf(stderr,"meta write lock\n");   
  // receive results.
  printf("Result: ");
  do {
    memset(rcv_buffer, 0, BUF_SIZE);
    numbytes = read_str_from_socket(socket_fd, rcv_buffer, BUF_SIZE);
    if (numbytes != 0)
      printf("%s", rcv_buffer); // print to stdout
  } while (numbytes > 0);
  printf("\n");
      
  // close the connection to the server.
  close(socket_fd);
  //pthread_mutex_unlock(&requests_lock);

}

void talk(const struct sockaddr_in server_addr, char *buffer) {
  char rcv_buffer[BUF_SIZE];
  int socket_fd, numbytes;
  char *buffer1="PUT:stations.125:6";  
  // create socket
  if ((socket_fd = socket(PF_INET, SOCK_STREAM, 0)) == -1) {
    ERROR("socket()");
  }

  // connect to the server.
  if (connect(socket_fd, (struct sockaddr*) &server_addr, sizeof(server_addr)) == -1) {
    ERROR("connect()");
  }
  
  // send message.
  write_str_to_socket(socket_fd, buffer1, strlen(buffer1));
      
  // receive results.
  printf("Result: ");
  do {
    memset(rcv_buffer, 0, BUF_SIZE);
    numbytes = read_str_from_socket(socket_fd, rcv_buffer, BUF_SIZE);
    if (numbytes != 0)
      printf("%s", rcv_buffer); // print to stdout
  } while (numbytes > 0);
  printf("\n");
      
  // close the connection to the server.
  close(socket_fd);
  exit(0);
}

/**
 * @name main - The main routine.
 */
int main(int argc, char **argv) {
  char *host = NULL;
  char *request = NULL;
  int mode = 0;
  int option = 0;
  int count = ITER_COUNT;
  char snd_buffer[BUF_SIZE];
  int station, value;
  
  struct hostent *host_info;
  
  // Parse user parameters.
  while ((option = getopt(argc, argv,"i:mhgpo:a:")) != -1) {
    switch (option) {
      case 'h':
        print_usage();
        exit(0);
      case 'a':
        host = optarg;
        break;
      case 'i':
        count = atoi(optarg);
	break;
      case 'g':
        if (mode) {
          fprintf(stderr, "You can only specify one of the following: -g, -p, -o\n");
          exit(EXIT_FAILURE);
        }
        mode = GET_MODE;
        break;
      case 'p': 
        if (mode) {
          fprintf(stderr, "You can only specify one of the following: -g, -p, -o\n");
          exit(EXIT_FAILURE);
        }
        mode = PUT_MODE;
        break;
      case 'o':
        if (mode) {
          fprintf(stderr, "You can only specify one of the following: -r, -w, -o\n");
          exit(EXIT_FAILURE);
        }
        mode = USER_MODE;
        request = optarg;
        break;
      case 'm':
        fprintf(stderr,"mmmmmmmmmm\n");
        if (mode) {
          fprintf(stderr, "You can only specify one of the following: -r, -w, -o,-m\n");
          exit(EXIT_FAILURE);
        }
        mode = MULTI_SENDER;
	 fprintf(stderr,"mmmmm1mmmmm\n");
        //request = optarg;
        break;
      default:
        print_usage();
        exit(EXIT_FAILURE);
    }
  }
   fprintf(stderr,"mmm2mmmmmmm\n");
  // Check parameters.
  if (!mode) {
    fprintf(stderr, "Error: One of -g, -p, -o is required.\n\n");
    print_usage();
    exit(0);
  }
  if (!host) {
    fprintf(stderr, "Error: -a <address> is required.\n\n");
    print_usage();
    exit(0);
  }
  
  // get the host (server) info
  if ((host_info = gethostbyname(host)) == NULL) { 
    ERROR("gethostbyname()"); 
  }
    fprintf(stderr,"mmmmm3mmmmm\n");
  // create socket adress of server (type, IP-adress and port number)
  bzero(&server_addr, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr = *((struct in_addr*)host_info->h_addr);
  server_addr.sin_port = htons(SERVER_PORT);
   fprintf(stderr,"mmmmm4mmmmm\n");
  if (mode == USER_MODE) {
     fprintf(stderr,"mmmmif1mmmmmm\n");
    memset(snd_buffer, 0, BUF_SIZE);
    strncpy(snd_buffer, request, strlen(request));
    printf("1Operation: %s\n", snd_buffer);
    talk(server_addr, snd_buffer);
  } else {
     fprintf(stderr,"mmmm else mmmmmm\n");
    while(--count>=0) {
       fprintf(stderr,"mmmm while mmmmmm\n");
      for (station = 0; station <= MAX_STATION_ID; station++) {
         fprintf(stderr,"mmmfor mmmmmmm\n");
        memset(snd_buffer, 0, BUF_SIZE);
        fprintf(stderr,"after memset mmmmmmm\n");
        if (mode == GET_MODE) {
          // Repeatedly GET.
           fprintf(stderr,"mmmmGETmmmmm\n");
          sprintf(snd_buffer, "GET:station.%d", station);
        } else if (mode == PUT_MODE) {
          // Repeatedly PUT.
           fprintf(stderr,"mmmPUTmmmmmmm\n");
          // create a random value.
          value = rand() % 65 + (-20);
          sprintf(snd_buffer, "PUT:station.%d:%d", station, value);
        }else if(mode== MULTI_SENDER){
	  	
		request_number=0;
               fprintf(stderr,"mmmmmMULTImmmmm\n");
               multi_task();
                
		
                 fprintf(stderr,"mmmmmENDmmmmm\n");
		break;
   		
		
	}
        printf("2Operation: %s\n", snd_buffer);
        talk(server_addr, snd_buffer);
      }
    }
  }
  return 0;
}

