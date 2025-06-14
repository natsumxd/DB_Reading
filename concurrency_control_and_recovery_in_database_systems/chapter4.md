#  CHAPTER 4 NON-LOCKING SCHEDULERS

## 4.2 TIMESTAMP ORDERING (TO)

the TM assigns a unique timestamp, $ts(T_i)$, to each transaction, $T_i$.  

**TO Rule:**  If  $p_i[x]$ and $q_j[x]$ are conflicting operations, then the DM processes $p_i[x]$ before $q_j[x]$ iff $\mathrm{ts}(T_i) < \mathrm{ts}(T_j)$.

The TO rule can produces SR executions.

#### Basic TO

**Basic TO Rule judged algorithm:**

*Algorithm:* $Basic\\_TO\\_schedule(o_i[x])$, *input:* $o_i[x]$ is $w_i[x]$ or $r_i[x]$.

*Data structure:*

Basic TO scheduler maintains following timestamp for every data item $x$:

* $\mathrm{max\mbox{-}r\mbox{-}scheduled}[x]$ : max timestamp of Reads on $x$;
* $\mathrm{max\mbox{-}w\mbox{-}scheduled}[x]$ : max timestamp of Writes on $x$;

*Procedure:*

* For Read operation $r_i[x]$:  If $\mathrm{ts}(T_i) >= \mathrm{max\mbox{-}w\mbox{-}sceduled}[x] $, then schedules $r_i[x]$ and set $\mathrm{max\mbox{-}r\mbox{-}scheduled}[x] = \mathrm{max}(\mathrm{ts}(T_i), \mathrm{max\mbox{-}r\mbox{-}scheduled[x]})$, else, reject $r_i[x]$ and abort $T_i$;

* For Write operation $w_i[x]$: If $\mathrm{ts}(T_i) >= \mathrm{max\mbox{-}w\mbox{-}sceduled}[x]$ and $\mathrm{ts}(T_i) >= \mathrm{max\mbox{-}r\mbox{-}sceduled}[x]$ , then schedules $w_i[x]$ and set $\mathrm{max\mbox{-}w\mbox{-}scheduled}[x] = \mathrm{ts}(T_i)$, else, reject $w_i[x]$ and abort $T_i$;
* Abort $T_i$ not only rollback the changes in DM but also need to rollback the changes in the $\mathrm{max\mbox{-}w\mbox{-}sceduled}[x]$  and $\mathrm{max\mbox{-}r\mbox{-}sceduled}[x]$ for each $x$ in $T_i$.

The scheduler must give handshake with the DM to guarantee that operations are processed by the DM in the order that the scheduler sent them.Even if the scheduler decides that $p_i[x]$ can be scheduled, it must not send it to the DM until every conflicting $q_j[x]$ that it previously sent has been acknowledged by the DM.

**Handshake between scheduler and DM algorithm:**

*Algorithm:* $send\\_DM(o_i[x])$, *input:* $o_i[x]$ is $w_i[x]$ or $r_i[x]$.

*Data structure:*

Basic TO scheduler mantains following counter for every data item $x$ participated in handshake:

* $\mathrm{r\mbox{-}in\mbox{-}transit}[x]$: the number of Reads that has been sent to DM, but not yet acknowledged by the DM;
* $\mathrm{w\mbox{-}in\mbox{-}transit}[x]$: the number of Writes that has been sent to DM, but not yet acknowledged by the DM.
* $\mathrm{queue}[x]$: the queue of operations can be scheduled but has not been sent to DM.

*Procedure:*

* After executed $TO\\_schedule(o_i[x])$, the operation $o_i$ is enqueued into $\mathrm{queue}[x]$;
* For head operation $o_i$ in $\mathrm{queue}[x]$:
  * For Read operation $r_i[x]$: if  $\mathrm{w\mbox{-}in\mbox{-}transit}[x] = 0$, then set $\mathrm{r\mbox{-}in\mbox{-}transit}[x] = \mathrm{r\mbox{-}in\mbox{-}transit}[x] + 1$ and send $r_i[x]$ to DM, else, delay execution until $\mathrm{w\mbox{-}in\mbox{-}transit}[x] = 0$.
  * For Write operation $w_i[x]$: if  $\mathrm{w\mbox{-}in\mbox{-}transit}[x] == 0$ and  $\mathrm{r\mbox{-}in\mbox{-}transit}[x] == 0$, then set $\mathrm{w\mbox{-}in\mbox{-}transit}[x] = 1$ and send $w_i[x]$ to DM, else, delay execution until $\mathrm{w\mbox{-}in\mbox{-}transit}[x] = 0$ and $\mathrm{r\mbox{-}in\mbox{-}transit}[x] = 0$.



#### Strict TO

**Problem:** Although the Basic TO can produces SR executions, it does not necessarily ensure recoverability. For example, suppose that $\mathrm{ts}( T_1) = 1$ and $\mathrm{ts}(T_2) = 2$, and consider the following history: $w_1[x], r_2[x], w_2[y] c_2$.Conflicting operations appear in timestamp order. Thus this history could be produced by Basic TO. Yet it is not recoverable: $T_2$ reads $x$ from $T_1$,  $T_2$ is committed, but $T_1$ is not.

In order to solve the RC and ACA problem, use the thought of 2PL to keep Strict.

*Algorithm:* $Strict\\_TO\\_schedule(o_i[x])$, *input:* $o_i[x]$ is $w_i[x]$ or $r_i[x]$.

*Procedure:*

Like 2PL, use $\mathrm{w\mbox{-}in\mbox{-}transit}[x]$ to represent a write lock, it does not set $\mathrm{w\mbox{-}in\mbox{-}transit}[x]$ to 0 when it receives the DM’s acknowledgment of $w_i[x]$. Instead it waits until it has received acknowledgment of $a_i$ or $c_i$, it then set $\mathrm{w\mbox{-}in\mbox{-}transit}[x]$ to 0 for every $x$ for which it had sent $w_i[x]$ to the DM.

#### Timestamp Management

**Problem:** Suppose we store timestamps in a table, where each entry is of the form $\{x, \mathrm{max\mbox{-}r\mbox{-}scheduled}[x], \mathrm{max\mbox{-}w\mbox{-}scheduled}[x]\}$ . This table could consume a lot of space.So we need purge the old entry periodically.

Suppose at any given time $t$, the scheduler can be pretty sure it won’t receive any more operations with timestamp smaller than $t - \delta$, where $\delta$ is large compared to transaction execution time. As a result, the entry in table must satisfy the condition: $\mathrm{max\mbox{-}r\mbox{-}scheduled}[x] < ts_{min}$  and $\mathrm{max\mbox{-}w\mbox{-}scheduled}[x] < ts_{min}$ where the $ts_{min} = t- \delta$.

When scheduler receive $o_i[x]$ from TM: it look for an entry for $x$ in the timestamp table. 

* if it find one, it then invoke the $Basic\\_TO\\_schedule(o_i[x])$ and  $send\\_DM(o_i[x])$；

* else, means that the entry has been purged, so scheduler need compare the $\mathrm{ts}(T_i)$ with $ts_{min}$;

  * if $\mathrm{ts}(T_i) < ts_{min}$, it reject $T_i$ because the timestamp of transaction $T_i$ is too old.
  * else, schedules the $o_i[x]$ because the entry must satisfy the condition: $\mathrm{max\mbox{-}r\mbox{-}scheduled}[x] < \mathrm{ts}(T_i)$  and $\mathrm{max\mbox{-}w\mbox{-}scheduled}[x] < \mathrm{ts}(T_i)$ before it has been purged.

  

#### Distributed TO schedulers

#### Conservative TO

**Problem:** In Basic TO and Strict TO, if scheduler receives operations in an order widely different from their timestamp order, then it may reject too many operations, thereby causing too many transactions to abort. 

**Delay execution algorithm:**

To solve this problem, One approach is to require the scheduler to artificially delay each operation it receives for some period of time. For example: at time $t$ , the scheduler receives an operation $w_i[x]$ and schedules it. after a while, at time $t+1$, the scheduler receives another operation $w_j[x]$, where $\mathrm{ts}(T_j) < \mathrm{ts}(T_i)$. When scheduling $w_j[x]$, since $\mathrm{ts}(T_j) < \mathrm{max\mbox{-}w\mbox{-}sceduled}[x]$ ,it needs to be rejected.If the scheduler delay the $w_i[x]$ until it has received acknowledgement for $w_j[x]$ from DM, the  $w_j[x]$ would not be rejected.

Of course, delaying operations for too long also has its problems, since the delays slow down the processing of transactions. When designing a conservative TO scheduler, one has to strike a balance by adding enough delay to avoid too many rejections without slowing down transactions too much.

 **“ultimate conservative” algorithm:**

Further, An “ultimate conservative” TO scheduler never rejects operations. It base on an assumption: In distribution, each TM submits its operations to each DM in timestamp order.

One way to satisfy ths assumption is to adopt the following achitecture: At any given time, each TM supervises exactly one transaction (e.g., there is one TM associated with each terminal from which users can initiate transactions) (*Note: each terminal will submit a transaction to a queue.Each time, the TM retrives the transaction at top of the queue for execution.*). Each TM’s timestamp generator returns increasing timestamps every time.

*Algorithm:* $conservative\\_TO\\_schedule(v, o_i[x])$, *input:* (1) $v$ is TM site ID; (2) $o_i[x]$ is $w_i[x]$ or $r_i[x]$.

*Data structure:*

* $\mathrm{unsched\mbox{-}queue}$: containing operations it has received from TMs but has not yet scheduled.
* $\mathrm{op\mbox{-}count}[v]$: the count of operations in unsched-queue received from $\mathrm{TM}_v$.

*Procedure:*

* The operations in $\mathrm{unsched\mbox{-}queue}$ are kept in  timestamp order and contains at least one operation for every TM, that said, must keep $\mathrm{op\mbox{-}count}[v] >= 1$.
* For head operation $o_i$ in $\mathrm{unsched\mbox{-}queue}$, if $\mathrm{op\mbox{-}count}[v] >= 1$, then invoke  $Basic\\_TO\\_schedule(o_i[x])$ and  $send\\_DM(o_i[x])$ and set $\mathrm{op\mbox{-}count}[v] = \mathrm{op\mbox{-}count}[v] - 1$.

**Optimization1:** In order to keep $\mathrm{op\mbox{-}count}[v] >= 1$, when TM is idle, it must sent a Null operation to schedule. In addition, when a TM fails, the sheduler should be informed (Note: if a TM shutdowns, it send a shutdonw packet to scheduler, else the scheduler will timeout because it have not receive the Null operation packet from the TM.)

**Optimization2:** In order to improve the performance, One approach is to require these transactions are divided into the number of TMs categories and each TM run the class transaction respectively by the readset and writeset. it demands that each transaction must predeclare its readset and writeset, so the system can direct it to an appropriate TM.



### 4.3 SERlALlZATiON GRAPH TESTlNG (SGT)

An SGT(serialization graph testing) scheduler maintains the SG of the history that represents the execution is controls. As the scheduler sends new operations to the DM, the execution changes, and so does the SG maintained by the scheduler. An SGT scheduler attains **SR** executions by ensuring the SG it maintains always remains acyclic.

#### Basic SGT

**Basic TO Rule judged algorithm:**

*Algorithm:* $Basic\\_SGT\\_schedule(o_i[x])$, *input:* $o_i[x]$ is $w_i[x]$ or $r_i[x]$.

*Data structure:*

Basic SGT scheduler maintains following writeset and readset for every operation $o_i[x]$ which have been scheduled.

* $\mathrm{SG\mbox{-}Node}[T_i]$: store the dependency relationship with other transactions, in addition, use different term $Stored \; SG(SSG$) to denote the $\mathrm{SG\mbox{-}Nodes}$ maintained by an SGT scheduler, suppose the amout of $\mathrm{SG\mbox{-}Nodes}$ is $n$.
* $\mathrm{r\mbox{-}scheduled}[T_i]$ : the Readsets of $T_i$，consist of: $\{r_{i}[x_1], r_i[x_2],...r_i[x_n]\}$;
* $\mathrm{w\mbox{-}scheduled}[T_i]$ : the writesets of $T_i$, consist of: $\{w_{i}[x_1], w_i[x_2],...w_i[x_n]\}$;

*Procedure:*

1. When an SGT scheduler receives an operation $o_i[x]$ from the TM, it first adds a $\mathrm{SG\mbox{-}Node}[T_i]$ for $T_i$ in $SSG$, if $T_i$ doesn’t already exist.
2. Determine dependency relationships for $T_i$:
   * For Read operation $r_i[x]$:  for each $\mathrm{w\mbox{-}scheduled}[T_j]$,  check whether $x\in \ \mathrm{w\mbox{-}scheduled}[T_j]$ is true,  if it is true, then add edge from $T_j$ to $T_i$. if $x \notin \bigcup_{j = 1}^{n} \mathrm{w\mbox{-}scheduled}[T_j]$ , then invoke $send\\_DM(r_i[x])$ ; else, it needs to check whether the $SSG$ is acyclic:
     * If the result of check is true, it adds the $x$ into $\mathrm{r\mbox{-}scheduled}[T_i]$ and invoke $send\\_DM(r_i[x])$;
     * else, abort $T_i$;
   * For Write operation $w_i[x]$: for each $\mathrm{w\mbox{-}scheduled}[T_j]$,  check whether $x\in \ \mathrm{w\mbox{-}scheduled}[T_j] \cup \mathrm{r\mbox{-}scheduled}[T_j]$ is true,  if it is true, then add edge from $T_j$ to $T_i$. else $x \notin \bigcup_{j = 1}^{n} \{\mathrm{w\mbox{-}scheduled}[T_j] \cup \mathrm{r\mbox{-}scheduled}[T_j] \}$ , then invoke $send\\_DM(w_i[x])$ ; else, it needs to check whether the $SSG$ is acyclic:
     * If the result of check is true, it adds the $x$ into $\mathrm{w\mbox{-}scheduled}[T_i]$ and invoke $send\\_DM(w_i[x])$;
     * else, abort $T_i$;
   * Abort $T_i$ not only rollback the changes in DM but also clean up the changes in the $\mathrm{w\mbox{-}sceduled}$ , $\mathrm{r\mbox{-}sceduled}$ and $SSG$.



**SSG Management**

A safe rule for deleting nodes is that information about a transaction may be discarded as soon as that transaction has terminated and is a source (i.e., a node with no incoming edges) in the SSG. One may naively assume that the scheduler can delete information about a transaction as soon as it commits. Unfortunately, this is not so. For example, consider the history:

$$
  H_2 = r_{k+1}[x]w_1[x]w_1[y_1]c_1w_2[x]w_2[y_2]c_2\; ... \; w_k[x]w_k[y_k]c_kw_{k+1}[y_2]
$$

If a node will be deleted as soon as it commits, above $T_1, T_2, \; ... \; T_k$ will be deleted by the scheduler. So the $w_{k+1}[y_2]$ will be scheduled through $Basic\\_ SGT\\_ schedule(w_{k+1}[y_2])$. But actually, the SSG dependency relationship of  $H_2$ is $T_2 \to T_{k+1} \to T_1 \to T_2 \to \; ...\; \to T_k$ and it has cycle: $T_2 \to T_{k+1} \to T_1 \to T_2$.

#### Conservative SGT

Like conservative TO, A conservative SGT scheduler never rejects operations but may delay them. As with 2PL and TO, we can achive this if each transaction $T_i$ predeclares its readset and writeset, denote $r\mbox{-}set[T_i]$ and $w\mbox{-}set[T_i]$, by attaching them to its Start operations. Like conservative TO, we can use $queue[x]$ to delay the operation $o_i[x]$ until the acknowlegement of conflict operations before $o_i[x]$ have received.

#### Recoverability Considerations

Like Strict TO, we can use use $\mathrm{w\mbox{-}in\mbox{-}transit}[x]$ to implement ST. it also can change some detail to implement the ACA and RC.

#### Distributed SGT Schedulers

Distributed SGT Scheduler present two problem:

1. The cycle in global SSG is not detected by local SSGs in each scheduler, For example, suppose that there are k sites, and $x_i$ is stored at site $i$, for $1=<i <=k$. and for site $i <k $, $T_i \to T_{i+1}$, but in global SSG, $T_1 \to T_2 \to ...\to T_k \to T_1$;

 $$
   H_3 = w_1[x_1]r_2[x_1]w_2[x_2]c_2 r_3[x_2]w_3[x_3]c_3\; ...\; r_k[x_{k-1}]w_k[x_k]c_k w_1[x_k]c_1
 $$

2. The global deadlock detection need spend more time than 2PL because 2PL just check the WFG but SGT must check every node.

#### *Space-Efficient SGT Schedulers





### 4.4 CERTIFIERS

In 2PL, TO, SGT, the scheduler must decide whether to accept, reject, or delay it when an operation is received from TM. A different approach is to have the scheduler immediately schedule each operation it receives. Then it checks to see what is has done. if it think all is well, it continues sheduling. else it must abort certain transactions.

**Definition:** When the transaction’s $T_i$ commit is sent to scheduler, it checks whether the $T_i$ is **SR**. if not , it rejects the commit, thereby forcing $T_i$ to abort.  Such scheduler are called certifiers. the process of checking whether a transaction’s commit can be safely scheduled or must be rejected is called certification.

There are certifiers based on all three types of schedulers - 2PL, TO, and SGT- with either centralized or distributed control. We will expiore all of these possibilities in this section.

#### 2PL Certification

*Algorithm:* $Certifier\\_2PL\\_schedule(c_i)$

*Data structure:*

2PL certification scheduler maintains following writeset and readset for every operation $o_i[x]$ which have been scheduled. these sets is similar to lockset.

* $\mathrm{active\mbox{-}transactions}[T_i]$: store the uncommitted transactions;
* $\mathrm{r\mbox{-}scheduled}[T_i]$ : the Readsets of $T_i$，consist of: $\\{ r_{i}[x_1], r_i[x_2],...r_i[x_n] \\}$;
* $\mathrm{w\mbox{-}scheduled}[T_i]$ : the writesets of $T_i$, consist of: $\\{w_{i}[x_1], w_i[x_2],...w_i[x_n]\\}$;

*Procedure:*

* At $T_i$ commit, for each transaction $T_j$ in $\mathrm{active\mbox{-}transactions}$, check if  **conflict condition**: $\mathrm{r\mbox{-}scheduled}[T_i] \cap \mathrm{w\mbox{-}scheduled}[T_j] \neq \\{ \\} \lor \mathrm{w\mbox{-}scheduled}[T_i] \cap \mathrm{w\mbox{-}scheduled}[T_j] \neq \\{ \\} \lor \mathrm{w\mbox{-}scheduled}[T_i] \cap \mathrm{r\mbox{-}scheduled}[T_j] \neq \\{ \\}$  is satisfied , it must abort $T_i$.

* In addition, to enfoce ST, if abort $T_i$, other transaction $T_k$ satisfied conflict condition with $T_i$ must be aborted.



#### SGT Certification

*Algorithm:* $Certifier\\_SGT\\_schedule(c_i)$

*Data structure:* like Basic SGT.

*Procedure:*

* At scheduler receive the operation $p_i[x]$, it adds the edge $T_j \to T_i$ to SSG for every transaction $T_j$ such that the scheduler has already sent to the DM an operation $q_j[x]$ conflicting $p_i[x]$. After this is done, it immediately dispatches $p_i[x]$ to the DM.
* When scheduler receives $c_i$, it checks where $T_i$ lie on a cycle of the SGT. if so, it reject $c_i$ and abort $T_i$. Otherwise it cerfies $T_i$ and commits $T_i$ normally.



#### TO Certification

*Algorithm:* $Certifier\\_TO\\_schedule(c_i)$

*Data structure:* like Basic TO.

*Procedure:*

* When scheduler receives $p_i[x]$, it ignore the fail of $Basic\\_TO\\_schedule(p_i[x])$, directly sent $p_i[x]$ to DM.
* When scheduler receives $c_i$, if it find a violation of TO rule, it immediately abort $T_i$.



#### Distributed Certifiers

In distributed environment, there are collection of certifier scheduler participated in a TM, one for each site. So a transaction concurrency control is decided by these schedulers. 

* For $SGT$,  when  schedulers participated $T_i$ receive $c_i$ from TM, the schedulers must exchange their local SSGs to ensure that the global SSG does not have a cycle involving the transaction being certified.
* For $TO$ and $2PL$, scheduler in each site can check whether $T_i$ has confliction. so the consensus algorithm have two phase: 
  * Phase1: TM send a $c_i$ to each scheduler and receive the result of the certified: $Certifier\\_TO\\_schedule(c_i)$ or $Certifier\\_2PL\\_schedule(c_i)$.
  * Phase2: if TM receive a certifier fail message from on scheduler, then the TM send “abort” to scheduler to abort $T_i$ for each sites, else it send “OK” to scheduler and the scheduler send $c_i$ to DM.



### 4.5 INTEGRATED SCHEDULERS

Firstly, we decompose the problem by separating the issue of scheduling Reads against conflicting Writes (and, symmetrically, of Writes against conflicting Reads) from that of scheduling Writes against conflicting Writes. We’ll call the first subproblem *RW synchronization* and the second, *WW synchronization.* 

Secondly, A complete scheduler consists of an rw and a ww synchronizer. The scheduler is called *Integrated  Scheduler.*

Thirdly, Integrated schedulers that use (possibly different versions of) the same mechanism (2PL, TO, or SGT) for both the rw and the ww synchronizer are called *pure schedulers.*Schedulers combining different mechanisms for rw and ww synchronization are called *mixed schedulers.*

Finally, The *serialization graph of history H,* $\mathrm{SG}(H)$ is decomposed into $\mathrm{SG_{rw}}(H)$ and $\mathrm{SG_{ww}}(H)$. So if the $\mathrm{SG}(H)$ is acyclic, both $\mathrm{SG_{rw}}(H)$ and $\mathrm{SG_{ww}}(H)$ must be acyclic. On the contrary. It is not enough for $\mathrm{SG_{rw}}(H)$ and $\mathrm{SG_{ww}}(H)$ to be acyclic, if we need $\mathrm{SG}(H)$ is acyclic. That is, the union of the two graphs must be acyclic.



#### Thomas’ Write Rule (TWR)

Thomas’ Write Rule states: Let $T_j$, be the transaction with timestamp $\mathrm{ts}(T_j)$ that wrote into $x$ before the scheduler receives $w_i[x]$. If $\mathrm{ts}(T_i) \geq \mathrm{ts}(T_j)$ process $w_i[x]$ as usual (submit it to the DM, wait for the DM’s ack($w_i[x]$), and then acknowledge it to the TM). Otherwise, process $w_i[x]$ by simply acknowledging it to the TM.

**TODO:** Prove.



#### A Pure Integrated Scheduler

Add Thomas’ Write Rule to Basic TO algorithm.



#### A Mixed Integrated Scheduler

A mixed integrated scheduler use 2PL for RW synchronization and TO for WW synchronization. To keep the TO rule, in $\mathrm{SG_{rw}}(H)$. it must be satisfy:

* If $T_i \to T_j$ is an edge of $\mathrm{SG_{rw}}(H)$, then $ts(T_i) > ts(T_j)$ to keep TO rule;
* To keep $ts(T_i) > ts(T_j)$, it need to commit $T_i$ before $T_j$ to keep the 2PL rule. 

To implement this, we need:

* Delay the allocation of timestamp until the transaction commit.
* Scheduler send write operations to DM until the transaction committed. To fix the RW synchronization and WW synchronization problem, it need store the write operations into the queue before the transaction commmitted.

In addition, to implement the timestamp allocation, it must consider the following two situation:

* In centerlization system, we only use a Incremental counter;
* In distribution system, to implement incremental counter, we need the following algorithm.



*Algorithm:* $Mixed\\_Integrated\\_schedule(c_i)$

*Data structure:* 

In Scheduler:

* $\mathrm{max\mbox{-}w\mbox{-}scheduled}[x]$ : max timestamp of Writes on $x$.($\mathrm{max\mbox{-}r\mbox{-}scheduled}[x]$ is unnecessary because the TO is not use for RW synchronization);
* The read and write locks in 2PL;

* The data structure in $send\\_DM(o_i[x])$;
* $\mathrm{max\mbox{-}ts}[x]$: the larger timestamp of transaction which have ever obtained a lock on $x$;

In TM:

* $\mathrm{max\mbox{-}lock\mbox{-}set}[T_i]$: the maximum timestamp of transaction $T_i$;

*Procedure:*

Update $\mathrm{max\mbox{-}lock\mbox{-}set}[T_i]$:

* TM send $o_i[x]$ to scheduler, then the scheduler
  * Firstly, obtain the lock on $x$: $ol_i[x]$ ;
  * Secondly, if $o_i[x]$ is $w_i[x]$ store it into queue else retrive  $x$ in the queue or send requestion to DM as memtion above;
  * Thirdly, release the lock on $x$: $ou_i[x]$;
* Scheduler response $\mathrm{max\mbox{-}ts}[x]$ to TM and update the $\mathrm{max\mbox{-}lock\mbox{-}set}[T_i] = \mathrm{max (max\mbox{-}lock\mbox{-}set}[T_i],\mathrm{max\mbox{-}ts}[x])$;

After receive the $c_i$:

* Firstly, the TM generate a timestamp greater than $\mathrm{max\mbox{-}lock\mbox{-}set}[T_i]$ as the timestamp of transaction $T_i$;
* For each $x$ which has been locked in $T_i$, update the $\mathrm{max\mbox{-}ts}[x] = \mathrm{max}(ts(T_i), \mathrm{max\mbox{-}ts}[x])$;
* Send all $w_i[x]$ of write queue in $T_i$ to DM. For each $w_i[x]$ in $T_i$, if $ts(T_i) >= \mathrm{max\mbox{-}w\mbox{-}scheduled}[x]$,  then send $w_i[x]$ to DM and update $\mathrm{max\mbox{-}w\mbox{-}scheduled}[x] = ts(T_i)$ ; else ignore it;
