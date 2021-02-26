# CS 448 Project 1 Important Changes

## BufferMgr.java
- changed access modifiers of `private` member to `protected`
- added method 
    ```java 
    public int getNumBuffs()
    ```
    - returns the length of `bufferpool`
    
## SimpleDB.java
- added method
    ```java
    public BufferMgr setBufferMgr(String className)
    ```
    - set the buffer manager of the databse to the specified `className`
    - new buffer manager has to `extends BufferMgr`
    
## Buffer.java

[comment]: <> (- added variable)

[comment]: <> (    ```java)

[comment]: <> (    private int reference = 0;)

[comment]: <> (    ```)

[comment]: <> (    - used in `BufferMgrClock`)
    
- added method
    ```java 
    public int getReference()
    ```
    - used in `BufferMgrClock`
    
- added method
    ```java
    public void resetReference()
    ```
    - used in `BufferMgrClock`
  
## CompareTime.java
- create a Buffer Manager under `simpledb/buffer`
- the new buffer should extend `BufferMgr`
- alter `BUFFERMGR_NEW` to the name of the new buffer manager
- alter `init` in `main` if necessary
```java
  public static void init(int bufferSize, int numRepeats)
```
### methods
```java
public static void runAll(int start, int end, int step)
```
- run all the tests starting with `_test`
- `start`, `end`, `step` is the range for `BUFFER_SIZE`
```java
public static void runRange(String test, int start, int end, int step)
```
- runs a specified test given its method name
- will write to a file under `project1out/tests`
```java
public static long[] compareTest(String test)
```
- returns the result of the test
  - `[0]`: original time
  - `[1]`: time using the new buffer manager
  
# graphing.py
- python version 3
- required packages
  - `matplotlib`
  - `numpy`
  - `pandas`
  
- alter `TESTNAME` and `BUFFERMGR` to the one in the test file
- run the script via
  ```shell
  python graphing.py
  ```