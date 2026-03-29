# Zenith

A functional stream processing language for data pipelines. Bytecode VM with generational GC, M:N fiber concurrency, and lazy streams -- implemented in Zig.

```zenith
-- Read JSONL, filter errors, transform, and write results
source("events.jsonl", :jsonl)
  |> filter_map(|r| {
    match r
      | Result.Ok(v) -> Option.Some(v)
      | Result.Err(_) -> Option.None
  })
  |> par_map(4, |event| transform(event))
  |> batch(1000)
  |> sink("output.jsonl", :jsonl)
```

## Features

- **Lazy streams** with pull-based backpressure -- infinite streams compose safely
- **Pipe operator** `|>` for natural left-to-right data flow
- **Parallel streams** -- `par_map` with configurable concurrency
- **M:N fiber concurrency** with channels and select
- **Algebraic data types** and exhaustive pattern matching
- **Result/Option types** with combinators for error handling
- **Closures** with upvalue capture and mutation
- **Tail call optimization** -- deep recursion without stack overflow
- **Generational GC** (nursery + old generation)
- **NaN-boxed values** -- no heap allocation for primitives
- **JSON/JSONL** encode/decode with streaming file I/O
- **Interactive REPL** with syntax coloring
- **Single binary** -- zero runtime dependencies

## Table of Contents

- [Getting Started](#getting-started)
- [CLI Reference](#cli-reference)
- [Language Guide](#language-guide)
  - [Comments](#comments)
  - [Types and Values](#types-and-values)
  - [Variables](#variables)
  - [Operators](#operators)
  - [Control Flow](#control-flow)
  - [Functions](#functions)
  - [Lambdas](#lambdas)
  - [Closures](#closures)
  - [Pipe Operator](#pipe-operator)
  - [Named Arguments](#named-arguments)
  - [Tail Call Optimization](#tail-call-optimization)
  - [Collections](#collections)
  - [Algebraic Data Types](#algebraic-data-types)
  - [Pattern Matching](#pattern-matching)
  - [Result and Option](#result-and-option)
  - [Streams](#streams)
  - [File I/O](#file-io)
  - [Parallel Streams](#parallel-streams)
  - [JSON](#json)
  - [Fibers](#fibers)
  - [Channels](#channels)
  - [Select](#select)
- [Standard Library](#standard-library)
- [Error Codes](#error-codes)
- [Architecture](#architecture)
- [License](#license)

## Getting Started

### Prerequisites

- [Zig](https://ziglang.org/) 0.15.2 or later

### Build

```sh
git clone https://github.com/eorlov/zenith.git
cd zenith
zig build
```

The binary is at `zig-out/bin/zenith`.

### Hello World

```zenith
-- hello.zen
print("hello, world")
```

```sh
zenith run hello.zen
```

### REPL

```sh
zenith repl
```

## CLI Reference

| Command | Description |
|---------|-------------|
| `zenith run <file.zen>` | Run a source file |
| `zenith run <file.znth>` | Run compiled bytecode |
| `zenith run -` | Read source from stdin |
| `zenith run -e '<expr>'` | Evaluate an inline expression |
| `zenith compile <file.zen>` | Compile to `.znth` bytecode |
| `zenith dis <file.znth>` | Disassemble bytecode |
| `zenith dis -v <file.znth>` | Verbose disassembly with debug info |
| `zenith repl` | Interactive REPL |
| `zenith explain <code>` | Explain an error code (e.g. `E001`) |
| `zenith --version` | Print version |

```sh
# Quick one-liner
zenith run -e 'print(range(1, 6) |> map(|x| x * x) |> collect())'

# Pipe from stdin
echo 'print(1 + 2)' | zenith run -
```

## Language Guide

### Comments

```zenith
-- Line comment
{- Block comment -}
```

### Types and Values

Zenith has six primitive types:

```zenith
print(type_of(42))       -- int
print(type_of(3.14))     -- float
print(type_of(true))     -- bool
print(type_of(nil))      -- nil
print(type_of("hello"))  -- string
print(type_of(:ok))      -- atom
```

**Strings** are UTF-8 and concatenated with `++`:

```zenith
print("hello" ++ " " ++ "world")  -- hello world
print(len("hello"))                -- 5
print(str(42))                     -- "42"
```

**Atoms** are lightweight interned symbols, useful as tags:

```zenith
let status = :ok
print(:ok == :ok)      -- true
print(:ok == :error)   -- false
```

### Variables

Variables are declared with `let`. Assignment with `=` rebinds the variable.

```zenith
let x = 42
print(x)         -- 42

-- Shadowing
let x = 100
print(x)         -- 100

-- Block scoping
{
  let z = 999
  print(z)       -- 999
}
print(x)         -- 100 (z is out of scope)

-- Blocks are expressions
let result = {
  let a = 10
  let b = 20
  a + b
}
print(result)    -- 30
```

### Operators

| Category | Operators |
|----------|-----------|
| Arithmetic | `+`, `-`, `*`, `/`, `%` |
| Comparison | `==`, `!=`, `<`, `>`, `<=`, `>=` |
| Logical | `and`, `or`, `not` |
| String concat | `++` |
| Pipe | `\|>` |
| Range | `..` |

```zenith
print(1 + 2 * 3)    -- 7  (standard precedence)
print((1 + 2) * 3)  -- 9
print(7 / 2)         -- 3  (integer division)
print(7 % 3)         -- 1
print(1.5 + 2.5)     -- 4.0
print(-42)           -- -42
```

### Control Flow

`if/else` is an expression -- it returns a value:

```zenith
let x = 10
let result = if x > 5 { "big" } else { "small" }
print(result)  -- big

-- Nested
let z = if x > 20 { "huge" } else { if x > 5 { "medium" } else { "tiny" } }
print(z)       -- medium
```

**Loops**:

```zenith
-- While loop
let i = 0
while i < 5 {
  print(i)
  i = i + 1
}

-- For-in with range
for j in range(3) { print(j) }           -- 0, 1, 2
for k in range(5, 8) { print(k) }        -- 5, 6, 7
for m in range(0, 10, 3) { print(m) }    -- 0, 3, 6, 9
```

### Functions

Functions are declared with `fn`. The last expression is the return value.

```zenith
fn add(a, b) { a + b }
print(add(1, 2))   -- 3

fn square(x) { x * x }
print(square(5))   -- 25

-- Explicit early return
fn early_return(x) {
  if x > 10 { return x }
  0
}
print(early_return(15))  -- 15
print(early_return(5))   -- 0
```

Functions are first-class values:

```zenith
-- Pass as argument
fn apply(f, x) { f(x) }
print(apply(square, 4))   -- 16

-- Return from function
fn make_adder(n) {
  fn adder(x) { x + n }
  adder
}
let add5 = make_adder(5)
print(add5(10))   -- 15

-- Store in variable
let f = add
print(f(3, 4))    -- 7

-- Recursive
fn factorial(n) {
  if n <= 1 { 1 } else { n * factorial(n - 1) }
}
print(factorial(5))  -- 120
```

### Lambdas

Lightweight anonymous functions with `|params| body`:

```zenith
let double = |x| x * 2
print(double(3))         -- 6

let add = |a, b| a + b
print(add(2, 3))         -- 5

-- Wildcard parameter
let const42 = |_| 42
print(const42())         -- 42

-- As argument
fn apply(f, x) { f(x) }
print(apply(|x| x + 10, 5))   -- 15

-- Immediately invoked
print((|x| x * x)(7))   -- 49

-- Captures variables from enclosing scope
let factor = 3
let mul = |x| x * factor
print(mul(4))            -- 12
```

### Closures

Functions capture variables from their enclosing scope by reference:

```zenith
-- Counter with mutable state
fn make_counter() {
  let count = 0
  fn increment() {
    count = count + 1
    count
  }
  increment
}
let c = make_counter()
print(c())   -- 1
print(c())   -- 2
print(c())   -- 3

-- Two closures sharing the same variable
fn make_pair() {
  let x = 0
  fn get() { x }
  fn set(val) { x = val }
  let g = get
  let s = set
  s(42)
  print(g())   -- 42
}
make_pair()

-- Closures survive enclosing scope exit
fn outer() {
  let x = "captured"
  fn inner() { x }
  inner
}
let f = outer()
print(f())   -- captured

-- Nested closures (two levels deep)
fn a() {
  let x = 1
  fn b() {
    let y = 2
    fn c() { x + y }
    c
  }
  b
}
print(a()()())   -- 3
```

### Pipe Operator

The `|>` operator passes the left side as the first argument to the right side:

```zenith
fn double(x) { x * 2 }
fn inc(x) { x + 1 }
fn add(a, b) { a + b }

print(5 |> double)          -- 10
print(5 |> add(3))          -- 8  (same as add(5, 3))
print(1 |> inc |> double |> inc)  -- 5

-- Lambda in parens
print(10 |> (|x| x * x))   -- 100

-- Multi-line chains
let result = 1
  |> inc
  |> double
  |> inc
print(result)   -- 5
```

### Named Arguments

Parameters with defaults are called by name:

```zenith
fn greet(name, greeting: "hello") {
  print(greeting)
  print(name)
}
greet("world")                  -- hello, world
greet("world", greeting: "hi") -- hi, world

fn config(host, port: 8080, debug: false) {
  print(host)
  print(port)
  print(debug)
}
config("localhost")
config("localhost", port: 9090, debug: true)

-- Works with pipe
fn format(value, prefix: "none") {
  print(prefix)
  print(value)
}
42 |> format(prefix: "answer")
```

### Tail Call Optimization

Recursive calls in tail position reuse the stack frame. Deep recursion doesn't overflow:

```zenith
fn count_down(n) {
  if n <= 0 { return 0 }
  count_down(n - 1)
}
print(count_down(100000))   -- 0 (no stack overflow)

-- Accumulator pattern
fn sum_tail(n, acc) {
  if n <= 0 { return acc }
  sum_tail(n - 1, acc + n)
}
print(sum_tail(1000, 0))    -- 500500
```

### Collections

#### Lists

Dynamic arrays. `List.*` operations return new lists (immutable semantics).

```zenith
let xs = [1, 2, 3]
print(xs)                          -- [1, 2, 3]
print([])                          -- []

-- List.get returns Option
print(List.get(xs, 0))             -- Option.Some(1)
print(List.get(xs, 10))            -- Option.None

-- Transform
print(List.map(xs, |x| x * 2))    -- [2, 4, 6]
print(List.filter([1,2,3,4,5], |x| x % 2 == 0))  -- [2, 4]
print(List.reduce(xs, 0, |acc, x| acc + x))       -- 6

-- Utilities
print(List.reverse(xs))            -- [3, 2, 1]
print(List.sort([3, 1, 2]))        -- [1, 2, 3]
print(List.contains(xs, 2))        -- true
print(List.append(xs, 4))          -- [1, 2, 3, 4]
print(xs)                          -- [1, 2, 3] (original unchanged)
print(len(xs))                     -- 3
```

#### Maps

Key-value collections with **quoted string keys**:

```zenith
let m = {"name": "alice", "age": 30}

-- Map.get returns Option
print(Map.get(m, "name"))          -- Option.Some(alice)
print(Map.get(m, "missing"))       -- Option.None

-- Immutable updates
let m2 = Map.set(m, "age", 31)
print(Map.get(m2, "age"))          -- Option.Some(31)
print(Map.get(m, "age"))           -- Option.Some(30) (original unchanged)

print(Map.delete(m, "age"))
print(Map.keys(m))                 -- [name, age]
print(Map.values(m))               -- [alice, 30]

-- Merge (right side wins on conflict)
let m4 = Map.merge({"a": 1}, {"b": 2, "a": 99})
print(Map.get(m4, "a"))            -- Option.Some(99)

print(Map.contains(m, "name"))     -- true
print(Map.length({}))              -- 0
```

#### Tuples

Fixed-size, heterogeneous, immutable:

```zenith
let point = (10, 20)
print(point)                       -- (10, 20)

-- Single-element: trailing comma required
let single = (42,)
print(single)                      -- (42,)

-- Without trailing comma, parentheses just group
let grouped = (42)
print(grouped)                     -- 42

-- Tuple.get returns Option
print(Tuple.get(point, 0))        -- Option.Some(10)
print(Tuple.get(point, 5))        -- Option.None

-- Heterogeneous types
let mixed = ("hello", 42, true)
print(mixed)                       -- (hello, 42, true)
```

#### Records

Named fields with **unquoted identifier keys** (distinct from maps):

```zenith
let person = {name: "alice", age: 30}
print(person)

-- Spread syntax for immutable updates
let older = {..person, age: 31}
print(older)
print(person)   -- original unchanged

-- Add new fields via spread
let detailed = {..person, email: "alice@example.com"}
print(detailed)

-- Nested records
let nested = {user: {name: "bob"}, active: true}
print(nested)
```

### Algebraic Data Types

Define sum types with `type`:

```zenith
type Color = | Red | Green | Blue | Hex(String)

-- Nullary constructors
let c1 = Color.Red
print(c1)                          -- Color.Red

-- With payload
let c2 = Color.Hex("#ff0000")
print(c2)                          -- Color.Hex(#ff0000)

-- Different ADT types are distinct
type Shape = | Circle(Float) | Rect(Float, Float)
let s = Shape.Circle(5.0)
print(s)
print(Shape.Rect(3.0, 4.0))

-- Recursive ADTs
type IntList = | Empty | Cons(Int, IntList)
let xs = IntList.Cons(1, IntList.Cons(2, IntList.Empty))
print(xs)

-- Equality
print(Color.Red == Color.Red)      -- true
print(Color.Red == Color.Green)    -- false
```

### Pattern Matching

The `match` expression destructures values with `|` arms. Supports guards, wildcards, and nested patterns.

```zenith
-- Literal patterns with guards
let grade = match 95
  | 100 -> "perfect"
  | n when n >= 90 -> "A"
  | n when n >= 80 -> "B"
  | _ -> "below B"
print(grade)   -- A

-- ADT destructuring
type Shape = | Circle(Float) | Rect(Float, Float)

let area = fn(s) {
  match s
    | Shape.Circle(r) -> 3.14159 * r * r
    | Shape.Rect(w, h) -> w * h
}
print(area(Shape.Circle(5.0)))     -- 78.53975
print(area(Shape.Rect(3.0, 4.0))) -- 12.0

-- Wildcard
let describe = fn(x) {
  match x
    | 0 -> "zero"
    | _ -> "nonzero"
}
print(describe(0))    -- zero
print(describe(42))   -- nonzero

-- Match is an expression
let x = match true | true -> 1 | false -> 0
print(x)   -- 1

-- Tuple patterns
let point = (10, 20)
match point
  | (x, y) -> print("x=" ++ str(x) ++ " y=" ++ str(y))

-- Record patterns
let person = {name: "Alice", age: 30}
match person
  | {name: n, age: a} -> print(n ++ " is " ++ str(a))

-- List patterns (exact length)
match [1, 2, 3]
  | [a, b, c] -> print("three: " ++ str(a) ++ "," ++ str(b) ++ "," ++ str(c))
  | _ -> print("other")

-- List patterns with rest
match [1, 2, 3, 4]
  | [first, ..rest] -> print("first=" ++ str(first) ++ " rest_len=" ++ str(List.length(rest)))
  | _ -> print("other")

-- Multi-arm matching
let describe_list = fn(lst) {
  match lst
    | [a, b, c] -> "three: " ++ str(a) ++ "," ++ str(b) ++ "," ++ str(c)
    | [a, b] -> "two: " ++ str(a) ++ "," ++ str(b)
    | [a] -> "one: " ++ str(a)
    | _ -> "other"
}
print(describe_list([10, 20, 30]))  -- three: 10,20,30
print(describe_list([5, 6]))        -- two: 5,6
print(describe_list([42]))          -- one: 42
```

### Result and Option

`Result` and `Option` are built-in ADTs for error handling without exceptions.

```zenith
-- Result: Ok or Err
let ok = Result.Ok(42)
let err = Result.Err("not found")

-- Combinators
print(Result.map_ok(ok, |x| x * 2))       -- Result.Ok(84)
print(Result.map_ok(err, |x| x * 2))      -- Result.Err(not found)
print(Result.map_err(err, |e| "Error: " ++ e))
print(Result.unwrap_or(ok, 0))             -- 42
print(Result.unwrap_or(err, 0))            -- 0
print(Result.is_ok(ok))                    -- true
print(Result.is_err(err))                  -- true

-- then (flatmap) for chaining fallible operations
let safe_div = fn(a, b) {
  match b
    | 0 -> Result.Err("division by zero")
    | _ -> Result.Ok(a / b)
}
print(Result.then(Result.Ok(10), |x| safe_div(x, 2)))   -- Result.Ok(5)
print(Result.then(Result.Ok(10), |x| safe_div(x, 0)))   -- Result.Err(division by zero)

-- Pipe-friendly chaining
let result = Result.Ok(10)
  |> Result.map_ok(|x| x + 5)
  |> Result.map_ok(|x| x * 2)
  |> Result.unwrap_or(0)
print(result)   -- 30
```

```zenith
-- Option: Some or None
let some = Option.Some("hello")
let none = Option.None

print(Option.map(some, |s| String.to_upper(s)))  -- Option.Some(HELLO)
print(Option.unwrap_or(some, "default"))          -- hello
print(Option.unwrap_or(none, "default"))          -- default
print(Option.is_some(some))                       -- true
print(Option.is_none(none))                       -- true
print(Option.to_result(some, "no value"))         -- Result.Ok(hello)
print(Option.to_result(none, "no value"))         -- Result.Err(no value)

-- Integration: List.get returns Option
let xs = [10, 20, 30]
let val = List.get(xs, 0) |> Option.unwrap_or(0)
print(val)       -- 10
let missing = List.get(xs, 99) |> Option.unwrap_or(0)
print(missing)   -- 0
```

**Pattern matching on Result/Option**:

```zenith
let handle = fn(r) {
  match r
    | Result.Ok(v) -> "got: " ++ str(v)
    | Result.Err(e) -> "error: " ++ e
}
print(handle(Result.Ok(42)))        -- got: 42
print(handle(Result.Err("oops")))   -- error: oops

-- filter_map pattern: extract Ok values, skip errors
let inputs = [Result.Ok(1), Result.Err("bad"), Result.Ok(3), Result.Err("fail"), Result.Ok(5)]
let ok_values = List.filter_map(inputs, |r| {
  match r
    | Result.Ok(v) -> Option.Some(v)
    | Result.Err(_) -> Option.None
})
print(ok_values)   -- [1, 3, 5]
```

### Streams

Streams are lazy, pull-based sequences. Transforms build a pipeline; terminals drive execution.

#### Sources

```zenith
-- Range
print(range(1, 6) |> collect())                        -- [1, 2, 3, 4, 5]

-- Infinite repetition
print(repeat(42) |> take(3) |> collect())               -- [42, 42, 42]

-- Infinite generator from seed
print(iterate(1, |x| x * 2) |> take(5) |> collect())   -- [1, 2, 4, 8, 16]
```

#### Transforms

All transforms are lazy -- they compose without evaluating.

```zenith
-- map: transform each element
print(range(1, 6) |> map(|x| x * 10) |> collect())
-- [10, 20, 30, 40, 50]

-- filter: keep matching elements
print(range(1, 11) |> filter(|x| x > 7) |> collect())
-- [8, 9, 10]

-- flat_map: one-to-many
print(range(1, 4) |> flat_map(|x| repeat(x) |> take(2)) |> collect())
-- [1, 1, 2, 2, 3, 3]

-- filter_map: map + filter (returns Option)
print(range(1, 6) |> filter_map(|x| {
  if x > 3 { Option.Some(x * 10) } else { Option.None }
}) |> collect())
-- [40, 50]

-- take / drop
print(range(1, 11) |> take(3) |> collect())   -- [1, 2, 3]
print(range(1, 11) |> drop(7) |> collect())   -- [8, 9, 10]

-- scan: running accumulation
print(range(1, 6) |> scan(0, |acc, x| acc + x) |> collect())
-- [1, 3, 6, 10, 15]

-- distinct: deduplicate
print(range(1, 4) |> flat_map(|x| repeat(x) |> take(2)) |> distinct() |> collect())
-- [1, 2, 3]

-- zip: pair two streams
print(range(1, 4) |> zip(range(10, 13)) |> collect())
-- [(1, 10), (2, 11), (3, 12)]

-- flatten: flatten nested lists
print(range(1, 4) |> map(|x| [x, x * 10]) |> flatten() |> collect())
-- [1, 10, 2, 20, 3, 30]

-- tap: side-effect without altering stream
let tap_result = range(1, 4) |> tap(|x| x) |> collect()
print(tap_result)   -- [1, 2, 3]

-- batch: group into chunks
print(range(1, 8) |> batch(3) |> collect())
-- [[1, 2, 3], [4, 5, 6], [7]]
```

#### Terminals

Terminals consume the stream and produce a final value.

```zenith
print(range(1, 11) |> sum())       -- 55
print(range(1, 6) |> reduce(0, |acc, x| acc + x))  -- 15
print(range(1, 6) |> first())      -- 1
print(range(1, 6) |> last())       -- 5
print(range(1, 6) |> min())        -- 1
print(range(1, 6) |> max())        -- 5
print(range(1, 11) |> count())     -- 10

-- each: consume with side-effect
range(1, 4) |> each(|x| print(x))  -- 1, 2, 3
```

#### Laziness and Backpressure

Streams are pull-based. `take(n)` on an infinite stream completes because downstream pulls only what it needs:

```zenith
-- This does NOT hang or run out of memory
let result = repeat(42) |> take(10) |> collect()
print(result)   -- [42, 42, 42, 42, 42, 42, 42, 42, 42, 42]

-- Infinite stream, squared, take first 5
let result2 = iterate(1, |x| x + 1) |> map(|x| x * x) |> take(5) |> collect()
print(result2)  -- [1, 4, 9, 16, 25]
```

#### partition_result

Split a `Stream(Result)` into two streams:

```zenith
let results = range(1, 6) |> map(|x| {
  if x % 2 == 0 { Result.Err(x) } else { Result.Ok(x) }
})
let parts = results |> partition_result()
print(parts.ok |> collect())    -- [1, 3, 5]
print(parts.err |> collect())   -- [2, 4]
```

### File I/O

```zenith
-- Read file line-by-line
print(source("data.txt") |> count())
print(source("data.txt") |> collect())

-- Read JSONL (each line parsed as JSON, wrapped in Result)
let records = source("data.jsonl", :jsonl) |> collect()
let first = Option.unwrap_or(List.get(records, 0), nil)
print(Result.is_ok(first))

-- JSONL with partition_result to separate valid/invalid records
let parts = source("data.jsonl", :jsonl) |> partition_result()
let ok_records = parts.ok |> collect()
let err_records = parts.err |> collect()

-- Write stream to file
range(1, 4) |> sink("/tmp/output.txt")

-- Write as JSONL
range(1, 4) |> sink("/tmp/output.jsonl", :jsonl)
```

### Parallel Streams

`par_map` processes elements concurrently using fibers:

```zenith
-- Parallel map with concurrency of 2 (preserves order)
let result = range(1, 6)
  |> par_map(2, |x| x * x)
  |> collect()
print(result)   -- [1, 4, 9, 16, 25]

-- Default concurrency (CPU count)
let result2 = range(1, 4)
  |> par_map(|x| x + 10)
  |> collect()
print(result2)  -- [11, 12, 13]

-- Unordered (completion order)
let result3 = range(1, 4)
  |> par_map_unordered(2, |x| x * 10)
  |> collect()
print(result3)  -- [10, 20, 30]

-- par_map_result: wraps in Result, errors don't crash the pipeline
let result4 = range(1, 4)
  |> par_map_result(2, |x| {
    if x == 2 { panic("bad") } else { x * 100 }
  })
  |> collect()
print(result4)  -- [Result.Ok(100), Result.Err(bad), Result.Ok(300)]

-- Chain par_map with other stream operators
let result5 = range(1, 11)
  |> par_map(2, |x| x * 2)
  |> filter(|x| x > 10)
  |> collect()
print(result5)  -- [12, 14, 16, 18, 20]

-- Extract Ok values, skip errors
let ok_vals = range(1, 4)
  |> par_map_result(2, |x| {
    if x == 2 { panic("skip") } else { x }
  })
  |> filter(|r| Result.is_ok(r))
  |> map(|r| Result.unwrap_or(r, nil))
  |> collect()
print(ok_vals)  -- [1, 3]
```

### JSON

```zenith
-- Decode (returns Result)
print(Result.unwrap_or(Json.decode("42"), nil))          -- 42
print(Result.unwrap_or(Json.decode("[1, 2, 3]"), nil))   -- [1, 2, 3]
print(Result.unwrap_or(Json.decode("true"), nil))        -- true
print(Result.unwrap_or(Json.decode("null"), 999))        -- nil
print(Result.is_err(Json.decode("not json")))            -- true

-- Encode (returns Result)
print(Result.unwrap_or(Json.encode(42), nil))            -- 42
print(Result.unwrap_or(Json.encode([1, 2, 3]), nil))     -- [1,2,3]
print(Result.unwrap_or(Json.encode("hello"), nil))       -- "hello"

-- Round-trip
print(Result.unwrap_or(Json.decode(Result.unwrap_or(Json.encode(42), "")), nil))  -- 42
```

### Fibers

Fibers are lightweight green threads scheduled M:N onto OS threads:

```zenith
-- Spawn and join
let f = spawn(|| 42)
let result = join(f)
print(result)   -- 42

-- Named fiber
let f2 = spawn("worker", || "hello from fiber")
print(join(f2))   -- hello from fiber

-- type_of
let f3 = spawn(|| nil)
print(type_of(f3))   -- fiber

-- Multiple fibers
let a = spawn(|| 1)
let b = spawn(|| 2)
let c = spawn(|| 3)
print(join(a))   -- 1
print(join(b))   -- 2
print(join(c))   -- 3
```

**Error isolation**: a fiber panic doesn't crash the parent.

```zenith
let f = spawn(|| panic("oops"))
let result = join(f)
print(result)          -- Result.Err(oops)
print("still running") -- main fiber continues
```

**Closures in fibers** capture variables correctly:

```zenith
let x = 42
let f = spawn(|| x)
print(join(f))   -- 42

let make_worker = |n| { || n * 2 }
let f2 = spawn(make_worker(21))
print(join(f2))  -- 42
```

### Channels

Channels provide inter-fiber communication with backpressure:

```zenith
-- Buffered channel
let ch = chan(2)
send(ch, 42)
send(ch, 43)
print(recv(ch))   -- 42
print(recv(ch))   -- 43

-- Unbuffered (rendezvous)
let ch3 = chan()
let sender = spawn(|| {
  send(ch3, "sync")
  close(ch3)
})
print(recv(ch3))  -- sync
join(sender)
```

**Producer-consumer with for-in iteration**:

```zenith
let ch = chan(3)
let writer = spawn(|| {
  send(ch, 10)
  send(ch, 20)
  send(ch, 30)
  close(ch)
})
for v in ch {
  print(v)       -- 10, 20, 30
}
join(writer)
```

### Select

Multiplex over multiple channels. The first ready arm wins:

```zenith
let ch1 = chan(1)
let ch2 = chan(1)
send(ch1, "first")
select {
  | recv(ch1) -> |val| print("ch1: " ++ str(val))
  | recv(ch2) -> |val| print("ch2: " ++ str(val))
}
-- ch1: first

-- Send arm
let ch3 = chan(1)
select {
  | send(ch3, "sent_via_select") -> print("sent")
}

-- Timeout
let ch4 = chan()
select {
  | recv(ch4) -> |val| print("got: " ++ str(val))
  | after(10) -> print("timeout")
}
-- timeout

-- Event loop pattern
let input = chan(3)
let done_ch = chan(1)
let worker = spawn(|| {
  send(input, 1)
  send(input, 2)
  send(input, 3)
  send(done_ch, :done)
})
let running = true
while running {
  select {
    | recv(input) -> |val| print("item: " ++ str(val))
    | recv(done_ch) -> |_| {
      running = false
      print("done")
    }
  }
}
join(worker)
```

## Standard Library

### Built-in Functions

| Function | Description |
|----------|-------------|
| `print(value)` | Print to stdout |
| `str(value)` | Convert to string |
| `len(value)` | Length (string bytes, list/map/tuple size) |
| `type_of(value)` | Type name as string |
| `assert(condition)` | Panic if false |
| `panic(message)` | Terminate with error |
| `range(n)` / `range(start, end)` / `range(start, end, step)` | Create range |
| `gc()` | Trigger garbage collection |
| `gc_stats()` | GC statistics record |

### List

| Function | Description |
|----------|-------------|
| `List.get(list, idx)` | Element at index (returns `Option`) |
| `List.set(list, idx, val)` | New list with element replaced |
| `List.append(list, val)` | New list with element appended |
| `List.length(list)` | Number of elements |
| `List.map(list, fn)` | Transform each element |
| `List.filter(list, fn)` | Keep elements matching predicate |
| `List.filter_map(list, fn)` | Map + filter (`fn` returns `Option`) |
| `List.reduce(list, init, fn)` | Fold left |
| `List.sort(list)` | Sorted copy |
| `List.reverse(list)` | Reversed copy |
| `List.zip(list1, list2)` | Pair elements into tuples |
| `List.flatten(list)` | Flatten nested lists |
| `List.contains(list, val)` | Membership test |

### Map

| Function | Description |
|----------|-------------|
| `Map.get(map, key)` | Value for key (returns `Option`) |
| `Map.set(map, key, val)` | New map with key set |
| `Map.delete(map, key)` | New map without key |
| `Map.keys(map)` | List of keys |
| `Map.values(map)` | List of values |
| `Map.merge(map1, map2)` | Merge (map2 wins on conflict) |
| `Map.contains(map, key)` | Key exists |
| `Map.length(map)` | Number of entries |

### String

| Function | Description |
|----------|-------------|
| `String.split(str, delim)` | Split into list |
| `String.trim(str)` | Remove leading/trailing whitespace |
| `String.join(list, delim)` | Join list with delimiter |
| `String.contains(str, substr)` | Substring test |
| `String.replace(str, old, new)` | Replace occurrences |
| `String.starts_with(str, prefix)` | Prefix test |
| `String.ends_with(str, suffix)` | Suffix test |
| `String.to_lower(str)` | Lowercase |
| `String.to_upper(str)` | Uppercase |
| `String.length(str)` | Byte count |

### Tuple

| Function | Description |
|----------|-------------|
| `Tuple.get(tuple, idx)` | Element at index (returns `Option`) |
| `Tuple.length(tuple)` | Number of elements |

### Result

| Function | Description |
|----------|-------------|
| `Result.Ok(value)` | Success constructor |
| `Result.Err(error)` | Error constructor |
| `Result.map_ok(result, fn)` | Transform Ok value |
| `Result.map_err(result, fn)` | Transform Err value |
| `Result.then(result, fn)` | Flatmap (`fn` returns `Result`) |
| `Result.unwrap_or(result, default)` | Extract value or use default |
| `Result.is_ok(result)` | Test for Ok |
| `Result.is_err(result)` | Test for Err |

### Option

| Function | Description |
|----------|-------------|
| `Option.Some(value)` | Value present |
| `Option.None` | Value absent |
| `Option.map(opt, fn)` | Transform Some value |
| `Option.unwrap_or(opt, default)` | Extract or use default |
| `Option.is_some(opt)` | Test for Some |
| `Option.is_none(opt)` | Test for None |
| `Option.to_result(opt, err)` | Convert to Result |

### Json

| Function | Description |
|----------|-------------|
| `Json.decode(string)` | Parse JSON string (returns `Result`) |
| `Json.encode(value)` | Serialize to JSON (returns `Result`) |

### Stream Operators

| Sources | Transforms | Terminals |
|---------|------------|-----------|
| `range(start, end)` | `map(fn)` | `collect()` |
| `repeat(value)` | `filter(fn)` | `sum()` |
| `iterate(init, fn)` | `flat_map(fn)` | `count()` |
| `source(path)` | `filter_map(fn)` | `reduce(init, fn)` |
| `source(path, :jsonl)` | `take(n)` / `drop(n)` | `first()` / `last()` |
| | `scan(init, fn)` | `min()` / `max()` |
| | `distinct()` | `each(fn)` |
| | `zip(stream)` | |
| | `flatten()` | |
| | `tap(fn)` | |
| | `batch(n)` | |
| | `partition_result()` | |
| | `par_map(n, fn)` | |
| | `par_map_unordered(n, fn)` | |
| | `par_map_result(n, fn)` | |

### Concurrency

| Function | Description |
|----------|-------------|
| `spawn(fn)` | Spawn a fiber |
| `spawn(name, fn)` | Spawn a named fiber |
| `join(fiber)` | Wait for fiber result |
| `chan(capacity)` | Create buffered channel |
| `chan()` | Create unbuffered channel |
| `send(ch, value)` | Send to channel |
| `recv(ch)` | Receive from channel |
| `close(ch)` | Close channel |
| `sink(path)` | Write stream to file |
| `sink(path, :jsonl)` | Write stream as JSONL |

## Error Codes

Zenith provides structured error codes with explanations:

| Code | Description |
|------|-------------|
| E001 | Type mismatch |
| E002 | Undefined variable |
| E003 | Integer overflow |
| E004 | Division by zero |
| E005 | Unexpected token |
| E006 | Unterminated string |
| E007 | Invalid number literal |
| E008 | Too many constants (>256 per function) |
| E009 | Too many locals (>256 per scope) |
| E010 | Break outside loop |
| E011 | Undefined atom |
| E012 | Arity mismatch |

```sh
zenith explain E001
```

## Architecture

Zenith compiles `.zen` source to bytecode and executes it on a stack-based VM:

```
Source (.zen)
    |
  Lexer       -- UTF-8 tokenizer
    |
  Parser      -- Recursive descent + Pratt precedence
    |
  AST         -- Flat MultiArrayList node pool
    |
  Compiler    -- Tree-walk bytecode emission
    |
  Chunk       -- Bytecode + constants + debug info (.znth)
    |
  VM          -- Labeled-switch interpreter
```

Key implementation details:

- **NaN-boxed values**: All values are 64-bit. Integers, booleans, nil, and atoms fit in NaN payload bits -- no heap allocation for primitives.
- **Generational GC**: Semi-space copying nursery for young objects, mark-sweep for old generation. Write barriers track inter-generational references.
- **M:N fiber scheduler**: Lightweight 4KB fiber stacks, work-stealing with Chase-Lev deques, one deque per OS thread.
- **Platform-specific context switching**: x86-64 and aarch64 assembly for fiber stack swaps.
- **Tail call optimization**: Tail calls reuse the current call frame (no stack growth).
- **String interning**: Identical strings share a single allocation via FNV-1a hash table.

## License

MIT
