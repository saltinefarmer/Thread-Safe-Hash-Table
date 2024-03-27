#include <limits.h>
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "ts_hashmap.h"

/**
 * Creates a new thread-safe hashmap. 
 *
 * @param capacity initial capacity of the hashmap.
 * @return a pointer to a new thread-safe hashmap.
 */
ts_hashmap_t *initmap(int capacity) {

  ts_hashmap_t *map = (ts_hashmap_t*) malloc(sizeof(ts_hashmap_t));
  map->table = (ts_entry_t**) malloc (sizeof(ts_entry_t*) * capacity);

  for(int i = 0; i < capacity; i++){
    map->table[i] = NULL;
  }

  map->numOps = 0;
  map->size = 0;
  map->capacity = capacity;

  map->locks = (pthread_mutex_t*) malloc(sizeof(pthread_mutex_t) * map->capacity);

  for(int i = 0; i < map->capacity; i++){
    pthread_mutex_init(&map->locks[i], NULL);
  }

  pthread_mutex_init(&map->size_lock, NULL);
  pthread_mutex_init(&map->numOps_lock, NULL);



  return map;
}

/**
 * Obtains the value associated with the given key.
 * @param map a pointer to the map
 * @param key a key to search
 * @return the value associated with the given key, or INT_MAX if key not found
 */
int get(ts_hashmap_t *map, int key) {

  int row = key % map->capacity;
  pthread_mutex_lock(&map->locks[row]);

  pthread_mutex_lock(&map->numOps_lock);
  map->numOps++;
  pthread_mutex_unlock(&map->numOps_lock);

  ts_entry_t *entry = &map->table[row][0];
  
  while(entry != NULL){
    if(entry->key == key){
      int temp = entry->value;
      pthread_mutex_unlock(&map->locks[row]);
      return temp;
    }
    entry = entry->next;
  }
  pthread_mutex_unlock(&map->locks[row]);

  return INT_MAX;
}

/**
 * Associates a value associated with a given key.
 * @param map a pointer to the map
 * @param key a key
 * @param value a value
 * @return old associated value, or INT_MAX if the key was new
 */
int put(ts_hashmap_t *map, int key, int value) {
  
  int row = key % map->capacity;
  pthread_mutex_lock(&map->locks[row]);

  pthread_mutex_lock(&map->numOps_lock);
  map->numOps++;
  pthread_mutex_unlock(&map->numOps_lock);
  
  ts_entry_t *entry = map->table[row];

  if(entry == NULL){

    ts_entry_t *new_item = (ts_entry_t*) malloc(sizeof(ts_entry_t));
    new_item->key = key;
    new_item->value = value;
    new_item->next = NULL;
    map->table[row] = new_item;

    pthread_mutex_lock(&map->size_lock);
    map->size++;
    pthread_mutex_unlock(&map->size_lock);


    pthread_mutex_unlock(&map->locks[row]);
    return INT_MAX;
  } else {
    if(entry->key == key){
        int temp = entry->value;
        entry->value = value;
        pthread_mutex_unlock(&map->locks[row]);
        return temp;
      }

    while(entry->next != NULL){
      entry = entry->next;
      if(entry->key == key){
        int temp = entry->value;
        entry->value = value;
        pthread_mutex_unlock(&map->locks[row]);
        return temp;
      }
    }

    ts_entry_t *new_item = (ts_entry_t*) malloc(sizeof(ts_entry_t));
    new_item->key = key;
    new_item->value = value;
    new_item->next = NULL;
    entry->next = new_item;

    pthread_mutex_lock(&map->size_lock);
    map->size++;
    pthread_mutex_unlock(&map->size_lock);

    pthread_mutex_unlock(&map->locks[row]);
    return INT_MAX;
  }
  
}

/**
 * Removes an entry in the map
 * @param map a pointer to the map
 * @param key a key to search
 * @return the value associated with the given key, or INT_MAX if key not found
 */
int del(ts_hashmap_t *map, int key) {
  int row = key % map->capacity;
  pthread_mutex_lock(&map->locks[row]);

  pthread_mutex_lock(&map->numOps_lock);
  map->numOps++;
  pthread_mutex_unlock(&map->numOps_lock);
  
  ts_entry_t *entry = &map->table[row][0];

  if(entry == NULL){
    pthread_mutex_unlock(&map->locks[row]);
    return INT_MAX;
  }

  while(entry->next != NULL){

    if(entry->next->key == key){
      ts_entry_t *next = entry->next;
      int temp = next->value;
      entry->next = next->next;
      free(next);

      pthread_mutex_lock(&map->size_lock);
      map->size--;
      pthread_mutex_unlock(&map->size_lock);

      pthread_mutex_unlock(&map->locks[row]);
      return temp;      
    }
    entry = entry->next;
  }
  pthread_mutex_unlock(&map->locks[row]);
  return INT_MAX;
}


/**
 * Prints the contents of the map (given)
 */
void printmap(ts_hashmap_t *map) {
  for (int i = 0; i < map->capacity; i++) {
    printf("[%d] -> ", i);
    ts_entry_t *entry = map->table[i];
    while (entry != NULL) {
      printf("(%d,%d)", entry->key, entry->value);
      if (entry->next != NULL)
        printf(" -> ");
      entry = entry->next;
    }
    printf("\n");
  }
}

/**
 * Free up the space allocated for hashmap
 * @param map a pointer to the map
 */
void freeMap(ts_hashmap_t *map) {
  
  // TODO: destroy locks

  for(int i = map->capacity -1; i >= 0 ; i--){
  
  pthread_mutex_destroy(&map->locks[i]);

    ts_entry_t *current = &map->table[i][0]; 
    ts_entry_t *next = NULL;
    if(current != NULL && current->next != NULL){
      next = current->next;
    }
    while(next != NULL){
      free(current);
      current = next;
      next = current->next;
    }
    if(current != NULL){
      free(current);
      current = NULL;
    }
  }
  free(map->locks);
  free(map->table);
  free(map);
}