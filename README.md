# MyBitcask
A implementation of log-structured fast kv databases based on the idea of Bitcask.
  
## Todo
**These are the basic features of the upcoming project**
- Support Redis Protocol
- Support Mutiple Data Structure
  - [ ] String
  - [ ] List
  - [ ] Set
  - [ ] Hash
  - [ ] Sorted Set
  - [ ] Maybe More..
- Support Mutiple Sync Mode
  - [ ] KeyValMemMode: Store both key and value in memory, faster in reading.
  - [ ] KeyOnlyMemMode: Store only key in memory, will search in disk for every reading.
- Support Mutiple Log File Mode
  - [ ] Standard File
  - [ ] MMap File
- Support Log File GC



## Reference
- https://medium.com/@arpitbhayani/bitcask-a-log-structured-fast-kv-store-c6c728a9536b
- https://github.com/flower-corp/rosedb
