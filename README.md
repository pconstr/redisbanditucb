redisbanditucb
===

A plain C Redis module with a (BANDITUCB) native data type that implements the [UCB algorithm](https://ieor8100.github.io/mab/Lecture%203.pdf) for (non-contextual) multi-armed bandits.

In this implementation ties are broken at random, and at the beginning all arms are pulled once in random order.


Structure and commands
===

Each (BanditUCB) key represents a bandit player following the UCB algorithm.

It needs to be initialised with the number of arms and with the scaling constant "c" for the UCB algorithm:


```
BANDIT.INIT <key> <arm> <c>
```

(square root of 2 is a common choice for "c" but it's really a tunable parameter)

Then it can pick an arm to pull:

```
BANDIT.PICK <key>
```

If two or more arms are tied (haven't been pulled yet or have the same bound) one will be drawn at random.


Bandits update themselves incrementally based on the rewards they obtain. It is not required that the update is for the arm it picked earlier,
or for PICK to be called at all before updates.

```
BANDIT.ADD <key> <arm> <reward>
```

Each BanditUCB (key) maintains reward counts and means, which are sufficient to compute the bound.

The UCB bound is used to compare arms after each has been pulled at least once.

These can be examined with `BANDIT.COUNTS`, `BANDIT.MEANS` and `BANDIT.BOUNDS`.

 Before an arm is pulled its bound will be `NaN`.

 It is also possible to set count and mean for an arm:

`BANDIT.SET <key> <arm> <count> <mean>`

That's used when rewriting the AOF.


Building and running
==

The Makefile works for me on Ubuntu 22.04 and MacOS Ventura with Redis from homebrew.

```
make
```

Redis can be configured to load the module like this:

```
loadmodule ./banditucb.so
```

Here I used a relative path as I run the server on the command line during development.

See also [example.txt](example.txt) and the [banditucb.c](banditucb.c)
