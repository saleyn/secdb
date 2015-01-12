//----------------------------------------------------------------------------
/// \file  secdb-writer.h
//----------------------------------------------------------------------------
/// \brief Multi-file asynchronous logger
///
/// The logger logs data to multiple streams asynchronously
/// It is optimized for performance of the producer to ensure minimal latency.
/// The producer of log messages never blocks during submission of a message.
//----------------------------------------------------------------------------
// Copyright (c) 2014 Omnibius, LLC
// Author:  Serge Aleynikov <saleyn@gmail.com>
// Created: 2012-03-20
//----------------------------------------------------------------------------
#pragma once
#include <assert.h>
#include <utxx/compiler_hints.hpp>
#include <utxx/time_val.hpp>
#include <utxx/synch.hpp>
#include <utxx/enum.hpp>
#include "secdb_api.h"

namespace secdb {
    using utxx::time_val;
    using utxx::futex;
    using utxx::posix_event;

/// Traits of asynchronous logger
struct SecdbWriterTraits {
    typedef std::allocator<char>      allocator;
    typedef std::allocator<char>      fixed_size_allocator;
    typedef futex                     event_type;
    static const int commit_timeout = 2000;  // commit this number of usec
};

/// Multi-stream asynchronous message logger
template<typename traits = SecdbWriterTraits>
struct SecdbWriter {
    /// Command sent to SecdbWriter by message producers
    struct Command;

    /// Stream information associated with a file descriptor
    /// used internally by the async logger
    class   StreamInfo;

    /// Internal stream identifier
    class   FileID;

    /// Custom destinations (other than regular files) can keep a pointer
    /// to their state associated with FileID structure
    class   stream_state_base {};

    typedef std::function<
        int (StreamInfo& a_si, int a_errno,
             const std::string& a_err)>             err_handler;

    typedef synch::posix_event                      close_event_type;
    typedef std::shared_ptr<close_event_type>       close_event_type_ptr;
    typedef typename traits::allocator::template
        rebind<char>::other                         msg_allocator;

private:
    typedef typename traits::fixed_size_allocator::template
        rebind<StreamInfo*>::other                  ptr_allocator;
    typedef std::set<
        StreamInfo*, StreamInfo_lt, ptr_allocator
    >                                               pending_data_streams_set;


    typedef std::vector<StreamInfo*>                StreamInfo_vec;
    typedef typename traits::fixed_size_allocator::template
        rebind<Command>::other                      cmd_allocator;

    std::mutex                                      m_mutex;
    std::condition_variable                         m_cond_var;
    std::shared_ptr<std::thread>                    m_thread;
    cmd_allocator                                   m_cmd_allocator;
    msg_allocator                                   m_msg_allocator;
    std::atomic<Command*>                           m_head;
    volatile bool                                   m_cancel;
    int                                             m_max_queue_size;
    std::atomic<long>                               m_total_msgs_processed;
    event_type                                      m_event;
    std::atomic<long>                               m_active_count;
    StreamInfo_vec                                  m_files;
    pending_data_streams_set                        m_pending_data_streams;
    int                                             m_last_version;
    err_handler                                     m_err_handler;

#ifdef PERF_STATS
    std::atomic<size_t>                             m_stats_enque_spins;
    std::atomic<size_t>                             m_stats_deque_spins;
#endif

    // Default output writer
    static int writev(StreamInfo& a_si, const char** a_categories,
                      const iovec* a_iovec, size_t a_sz);

    // Register a file or a stream with the logger
    FileID internal_register_stream(
        const std::string&  a_name,
        msg_writer          a_writer,
        stream_state_base*  a_state,
        int                 a_fd);

    bool internal_update_stream(StreamInfo* a_si, int a_fd);

    // Invoked by the async thread to flush messages from queue to file
    int  commit(const struct timespec* tsp = NULL);
    // Invoked by the async thread
    void run();
    // Enqueues msg to internal queue
    int  internal_enqueue(Command* a_cmd, const StreamInfo* a_si);
    // Writes data to internal queue
    int  internal_write(const FileID& a_id, const std::string& a_category,
                        char* a_data, size_t a_sz, bool copied);

    void internal_close();
    void internal_close(StreamInfo* p, int a_errno = 0);

    template <typename Msg>
    Command* allocate_message(const StreamInfo* a_si, const Msg* a_msg, size_t a_size)
    {
        Command* p = m_cmd_allocator.allocate(1);
        ASYNC_TRACE(("Allocated message (size=%lu): %p\n", a_size, p));
        new (p) Command(a_si, a_msg, a_size);
        return p;
    }

    Command* allocate_command(typename Command::Type a_tp, const StreamInfo* a_si) {
        Command* p = m_cmd_allocator.allocate(1);
        ASYNC_TRACE(("Allocated command (type=%d): %p\n", a_tp, p));
        new (p) Command(a_tp, a_si);
        return p;
    }

    void deallocate_command(Command* a_cmd);

    // Write enqueued messages from a_si->begin() till a_end
    int do_writev_and_free(StreamInfo* a_si, Command* a_end,
                           const char* a_categories[],
                           const iovec* a_wr_iov, size_t a_sz);

    bool check_range(int a_fd) const {
        return likely(a_fd >= 0 && (size_t)a_fd < m_files.size());
    }
    bool check_range(const FileID& a_id) const {
        if (!a_id) return false;
        int fd = a_id.fd();
        return check_range(fd) && m_files[fd] && a_id.version() == m_files[fd]->version;
    }
public:
    /// Create instance of this logger
    /// @param a_max_files is the max number of file descriptors
    /// @param a_reconnect_msec is the stream reconnection delay
    /// @param alloc is the message allocator to use
    explicit SecdbWriter(
        size_t a_max_files = 1024,
        int    a_reconnect_msec = 5000,
        const msg_allocator& alloc = msg_allocator());

    ~SecdbWriter() {
        stop();
    }

    /// Initialize and start asynchronous file writer
    /// @param filename     - name of the file
    /// @param max_msg_size - expected maximum message size (used to allocate
    ///                       the buffer on the stack to format the
    ///                       output string when calling fwrite function)
    /// @param mode         - set file access mode
    int  start();

    /// Stop asynchronous file writering thread
    void stop();

    /// Returns true if the async logger's thread is running
    bool running() { return m_thread.get(); }

    /// Start a new log file
    /// @param a_filename is the name of the output file
    /// @param a_append   if true the file is open in append mode
    /// @param a_mode     file permission mode (default 660)
    FileID open_file
    (
        const std::string& a_filename,
        bool               a_append = true,
        int                a_mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP
    );

    /// Same as open_file() but throws io_error exception on error
    FileID open_file_or_throw
    (
        const std::string& a_filename,
        bool               a_append = true,
        int                a_mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP
    );

    /// Start a new logging stream
    ///
    /// The logger won't write any data to file but will call \a a_writer
    /// callback on every iovec to be written to stream. It's the caller's
    /// responsibility to perform the actual writing.
    /// @param a_name   is the name of the stream
    /// @param a_writer a callback executed on writing messages to stream
    /// @param a_state  custom state available to the implementation's writer
    /// @param a_fd     is an optional file descriptor associated with the stream
    FileID open_stream
    (
        const std::string&  a_name,
        msg_writer          a_writer,
        stream_state_base*  a_state = NULL,
        int                 a_fd = -1
    );

    /// Same as open_stream() but throws io_error exception on error
    FileID open_stream_or_throw
    (
        const std::string&  a_name,
        msg_writer          a_writer,
        stream_state_base*  a_state = NULL,
        int                 a_fd = -1
    );

    /// This callback will be called from within the logger's thread
    /// to format a message prior to writing it to a log file. The formatting
    /// function can modify the content of \a a_data and if needed can reallocate
    /// and rewrite \a a_data and \a a_size to point to some other memory. Use
    /// the allocate() and deallocate() functions for this purpose. You should
    /// call this function immediately after calling open_file() and before
    /// writing any messages to it.
    void set_formatter(FileID& a_id, msg_formatter a_formatter);

    /// This callback will be called from within the logger's thread
    /// to write an array of iovec structures to a log file. You should
    /// call this function immediately after calling open_file() and before
    /// writing any messages to it.
    void set_writer(FileID& a_id, msg_writer a_writer);

    /// This callback will be called from within the logger's thread
    /// to report a write error on a given FileID. You should
    /// call this function immediately after calling open_file() and before
    /// writing any messages to it.
    void set_error_handler(const err_handler& a_handler);

    /// Set the size of a batch used to write messages to file using on_write
    /// function. Valid value is between 1 and IOV_MAX.
    /// Call this function immediately after calling open_file() and before
    /// writing any messages to it.
    void set_batch_size(FileID& a_id, size_t a_size);

    /// Set a callback for reconnecting to stream
    void set_reconnect(FileID& a_id, stream_reconnecter a_reconnector);

    /// Close one log file
    /// @param a_id identifier of the file to be closed. After return the value
    ///             will be reset.
    /// @param a_immediate indicates whether to close file immediately or
    ///             first to write all pending data in the file descriptor's
    ///             queue prior to closing it
    /// @param a_notify_when_closed is the event that, if not NULL, will be
    ///             signaled when the file descriptor is eventually closed.
    /// @param a_wait_secs is the number of seconds to wait until the file is
    ///             closed. -1 means indefinite.
    int close_file(FileID& a_id, bool a_immediate = false,
                   int a_wait_secs = -1);

    /// @return last error of a given file
    int last_error(const FileID& a_id) const {
        return check_range(a_id) ? m_files[a_id.fd()]->error : -1;
    }

    template <class T>
    T* allocate() {
        T* p = reinterpret_cast<T*>(m_msg_allocator.allocate(sizeof(T)));
        ASYNC_TRACE(("+allocate<T>(%lu) -> %p\n", sizeof(T), p));
        return p;
    }

    /// Allocate a message that can be passed to the loggers' write() function
    /// as its \a a_data argument
    char* allocate(size_t a_sz) {
        char* p = m_msg_allocator.allocate(a_sz);
        ASYNC_TRACE(("+allocate(%lu) -> %p\n", a_sz, p));
        return p;
    }

    /// Deallocate a message previously allocated by the call to allocate()
    void deallocate(char* a_data, size_t a_size) {
        ASYNC_TRACE(("-Deallocating msg(%p, %lu)\n", a_data, a_size));
        m_msg_allocator.deallocate(a_data, a_size);
    }

    /// Formatted write with argumets.  The a_data must have been
    /// previously allocated using the allocate() function.
    /// The logger will implicitely own the pointer and
    /// will have the deallocation responsibility.
    int write(const FileID& a_id, const std::string& a_category, void* a_data, size_t a_sz);

    /// Write a copy of the string a_data to a file.
    int write(const FileID& a_id, const std::string& a_category, const std::string& a_msg);

    /// @return max size of the commit queue
    const int   max_queue_size()        const { return m_max_queue_size; }
    const long  total_msgs_processed()  const { return m_total_msgs_processed
                                                .load(std::memory_order_relaxed); }
    const int   open_files_count()      const { return m_active_count
                                                .load(std::memory_order_relaxed); }
    /// Signaling event that can be used to wake up the logging I/O thread
    const event_type& event()           const { return m_event; }

    /// True when the logger has unprocessed data in its queue
    bool  has_pending_data()            const { return m_head
                                                .load(std::memory_order_relaxed); }
#ifdef PERF_STATS
    size_t stats_enque_spins()           const { return m_stats_enque_spins
                                                .load(std::memory_order_relaxed); }
    size_t stats_deque_spins()           const { return m_stats_deque_spins
                                                .load(std::memory_order_relaxed); }
#endif

};

/// Default implementation of multi_file_async_logger
typedef SecdbWriter<> multi_file_async_logger;

//-----------------------------------------------------------------------------
// Local classes
//-----------------------------------------------------------------------------

template<typename traits>
struct SecdbWriter<traits>::
Command {
    UTXX_DEFINE_ENUM
    (Type,
        MDS,            // Send MD Snapshot message
        Trade,          // Send Trade message
        Close,          // Close stream
        DestroyStream   // Destroy stream object
    );

    union UData {
        MDSnapshot* mds;
        Trade*      trade;
        struct {
            bool    immediate;
        } close;

        UData()  {}
        ~UData() {}
    };

    const Type          type;
    const StreamInfo*  stream;
    UData               args;
    mutable Command*    next;
    mutable Command*    prev;

    Command(const StreamInfo* a_si, const MDSnapshot* a_data)
        : type(Type::MDS), stream(a_si), next(NULL), prev(NULL)
    {
        args.mds = a_data;
    }

    Command(const StreamInfo* a_si, const Trade* a_data)
        : type(Type::Trade), stream(a_si), next(NULL), prev(NULL)
    {
        args.trade = a_data;
    }

    Command(Type a_type, const StreamInfo* a_si)
        : type(a_type), stream(a_si), next(NULL), prev(NULL)
    {
        assert(a_type != Type::MDS && a_type != Type::Trade);
    }

    ~Command() {
        if (type == msg)
            args.msg.category.~basic_string();
    }

    int fd() const { return stream->fd; }

    void unlink() {
        if (prev) prev->next = next;
        if (next) next->prev = prev;
    }
} __attribute__((aligned(UTXX_CL_SIZE)));

template<typename traits>
class SecdbWriter<traits>::
FileID {
    StreamInfo* m_stream;
public:
    FileID()                { reset(); }
    FileID(StreamInfo* a_si) : m_stream(a_si) {}
    bool invalid() const    { return !m_stream || m_stream->fd < 0; }
    int  version() const    { assert(m_stream); return m_stream->version;  }
    int  fd()      const    { return likely(m_stream) ? m_stream->fd : -1; }

    StreamInfo*       stream()        { return m_stream; }
    const StreamInfo* stream() const  { return m_stream; }

    void              reset()         { m_stream = NULL; }
    bool              operator==(const FileID& a) { return m_stream && a.m_stream; }

    operator          bool()   const  { return !invalid(); }
};

/// Stream information associated with a file descriptor
/// used internally by the async logger
template<typename traits>
class SecdbWriter<traits>::
StreamInfo {
    SecdbWriter<traits>* m_logger;
    // This transient list stores commands that are to be written
    // to the stream represented by this StreamInfo structure
    Command*             m_pending_writes_head;
    Command*             m_pending_writes_tail;

    // Time of last reconnect attempt
    close_event_type_ptr on_close;      // Event to signal on close

    template <typename T> friend struct SecdbWriter;

public:
    std::string          name;
    int                  fd;
    int                  error;
    std::string          error_msg;
    int                  version;       // Version number assigned when file is opened.
    size_t               max_batch_sz;  // Max number of messages to be batched
    stream_state_base*   state;

    explicit StreamInfo(stream_state_base* a_state = NULL);

    StreamInfo(SecdbWriter<traits>* a_logger,
               const std::string& a_name, int a_fd, int a_version,
               stream_state_base* a_state = NULL);

    ~StreamInfo() { reset(); }

    static iovec def_on_format(const std::string& a_category, iovec& a_msg)
      { return a_msg; }

    void reset(int a_errno = 0);

    StreamInfo* reset(const std::string& a_name, msg_writer a_writer,
                      stream_state_base* a_state, int a_fd);

    void set_error(int a_errno, const char* a_err = NULL);

    /// Push a list of commands to the internal pending queue in reverse order
    ///
    /// The commands are pushed as long as they are destined to this stream.
    /// This method is not thread-safe, it's meant for internal use.
    /// @return number of commands enqueued. Upon return \a a_cmd is updated
    ///         with the first command not belonging to this stream or NULL if no
    ///         such command is found in the list
    int push(const Command*& a_cmd);

    /// Returns true of internal queue is empty
    bool           pending_queue_empty() const     { return !m_pending_writes_head;   }
    Command*&      pending_writes_head()           { return m_pending_writes_head;    }
    const Command* pending_writes_tail() const     { return m_pending_writes_tail;    }
    void           pending_writes_head(Command* a) { m_pending_writes_head = a;       }
    void           pending_writes_tail(Command* a) { m_pending_writes_tail = a;       }

    const time_val& last_reconnect_attempt() const { return m_last_reconnect_attempt; }

    /// Erase single \a item command from the internal queue
    void erase(Command* item);

    /// Erase commands from \a first till \a end from the internal
    /// queue of pending commands
    void erase(Command* first, const Command* end);
};

//-----------------------------------------------------------------------------
// Implementation: StreamInfo
//-----------------------------------------------------------------------------

template<typename traits>
SecdbWriter<traits>::
StreamInfo::StreamInfo(stream_state_base* a_state)
    : m_logger(NULL)
    , m_pending_writes_head(NULL), m_pending_writes_tail(NULL)
    , on_format(&StreamInfo::def_on_format)
    , on_write(&SecdbWriter<traits>::writev)
    , fd(-1), error(0), version(0), max_batch_sz(IOV_MAX)
    , state(a_state)
{}

template<typename traits>
SecdbWriter<traits>::
StreamInfo::StreamInfo(
    SecdbWriter<traits>* a_logger,
    const std::string& a_name, int a_fd, int a_version,
    msg_writer a_writer,
    stream_state_base* a_state
)   : m_logger(a_logger)
    , m_pending_writes_head(NULL), m_pending_writes_tail(NULL)
    , on_format(&StreamInfo::def_on_format)
    , on_write(a_writer)
    , name(a_name), fd(a_fd), error(0)
    , version(a_version), max_batch_sz(IOV_MAX)
    , state(a_state)
{}

template<typename traits>
void SecdbWriter<traits>::
StreamInfo::reset(int a_errno) {
    ASYNC_TRACE(("Resetting stream %p (fd=%d)\n", this, fd));
    state = NULL;

    if (a_errno >= 0)
        set_error(a_errno, NULL);

    if (fd != -1) {
        (void)::close(fd);
        fd = -1;
    }

    if (on_close) {
        on_close->signal();
        on_close.reset();
    }
}

template<typename traits>
typename SecdbWriter<traits>::StreamInfo*
SecdbWriter<traits>::
StreamInfo::reset(const std::string& a_name, msg_writer a_writer,
                   stream_state_base* a_state, int a_fd)
{
#ifdef PERF_STATS
    m_stats_enque_spins.store(0u);
    m_stats_deque_spins.store(0u);
#endif
    name     = a_name;
    fd       = a_fd;
    error    = 0;
    state    = a_state;
    on_write = a_writer;
    return this;
}

template<typename traits>
void SecdbWriter<traits>::
StreamInfo::set_error(int a_errno, const char* a_err) {
    error_msg = a_err ? a_err : (a_errno ? errno_string(a_errno) : std::string());
    error     = a_errno;
}

template<typename traits>
int SecdbWriter<traits>::
StreamInfo::push(const Command*& a_cmd) {
    int n = 0;
    // Reverse the list
    Command* p = const_cast<Command*>(a_cmd), *last = NULL;
    for (; p && p->stream == this; ++n) {
        p->prev = p->next;
        p->next = last;
        last    = p;
        p       = p->prev; // Former p->next

        ASYNC_TRACE(("  FD[%d]: caching cmd (tp=%s) %p (prev=%p, next=%p)\n",
                        fd, last->type_str(), last, last->prev, last->next));
    }

    if (!last)
        return 0;

    last->prev = m_pending_writes_tail;

    if (!m_pending_writes_head)
        m_pending_writes_head = last;

    if (m_pending_writes_tail)
        m_pending_writes_tail->next = last;

    m_pending_writes_tail = const_cast<Command*>(a_cmd);

    ASYNC_TRACE(("  FD=%d cache head=%p tail=%p\n", fd,
                    m_pending_writes_head, m_pending_writes_tail));

    a_cmd = p;

    return n;
}

template<typename traits>
void SecdbWriter<traits>::
StreamInfo::erase(Command* item) {
    if (m_pending_writes_head == item) m_pending_writes_head = item->next;
    if (m_pending_writes_tail == item) m_pending_writes_tail = item->prev;
    if (item->prev) item->prev->next = item->next;
    if (item->next) item->next->prev = item->prev;
    m_logger->deallocate_command(item);
}

/// Erase commands from \a first till \a end from the internal
/// queue of pending commands
template<typename traits>
void SecdbWriter<traits>::
StreamInfo::erase(Command* first, const Command* end) {
    ASYNC_TRACE(("xxx StreamInfo(%p)::erase: purging items [%p .. %p) from queue\n",
                 this, first, end));
    for (Command* p = first, *next; p != end; p = next) {
        next = p->next;
        m_logger->deallocate_command(p);
    }
    if (end) end->prev = NULL;
}

//-----------------------------------------------------------------------------
// Implementation: SecdbWriter
//-----------------------------------------------------------------------------

template<typename traits>
SecdbWriter<traits>::
SecdbWriter(
    size_t a_max_files, int a_reconnect_msec, const msg_allocator& alloc)
    : m_thread(nullptr)
    , m_msg_allocator(alloc)
    , m_head(nullptr)
    , m_cancel(false)
    , m_max_queue_size(0)
    , m_total_msgs_processed(0)
    , m_event(0)
    , m_active_count(0)
    , m_files(a_max_files, nullptr)
    , m_last_version(0)
    , m_reconnect_sec((double)a_reconnect_msec / 1000)
#ifdef PERF_STATS
    , m_stats_enque_spins(0)
    , m_stats_deque_spins(0)
#endif
{}

template<typename traits>
inline int SecdbWriter<traits>::
writev(StreamInfo& a_si, const char** a_categories, const iovec* a_iovec, size_t a_sz)
{
#ifdef PERF_NO_WRITEV
    return a_sz;
#else
    return a_si.fd < 0 ? 0 : ::writev(a_si.fd, a_iovec, a_sz);
#endif
}

template<typename traits>
int SecdbWriter<traits>::
start() {
    std::unique_lock<std::mutex> lock(m_mutex);

    if (running())
        return -1;

    m_event.reset();
    m_cancel = false;

    m_thread.reset(
        new std::thread(
            std::bind(&SecdbWriter<traits>::run, this))
    );

    m_cond_var.wait(lock);

    return 0;
}

template<typename traits>
void SecdbWriter<traits>::
stop() {
    if (!running())
        return;

    ASYNC_TRACE((">>> Stopping async logger (head %p)\n", m_head.load()));

    std::shared_ptr<std::thread> t = m_thread;
    if (t) {
        m_cancel = true;
        m_event.signal();

        t->join();
    }
}

template<typename traits>
void SecdbWriter<traits>::
run() {
    // Notify the caller that we are ready
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_cond_var.notify_all();
    }

    ASYNC_TRACE(("Started async logging thread (cancel=%s)\n",
        m_cancel ? "true" : "false"));

    static const timespec ts =
        {traits::commit_timeout / 1000, (traits::commit_timeout % 1000) * 1000000 };

    m_total_msgs_processed = 0;

    while (true) {
        #if defined(DEBUG_ASYNC_LOGGER) && DEBUG_ASYNC_LOGGER != 2
        int rc =
        #endif
        commit(&ts);

        ASYNC_TRACE(( "Async thread commit result: %d (head: %p, cancel=%s)\n",
            rc, m_head.load(), m_cancel ? "true" : "false" ));

        // CPU-friendly spin for 250us
        time_val deadline(rel_time(0, 250));
        while (!m_head.load(std::memory_order_relaxed)) {
            if (m_cancel)
                goto DONE;
            if (now_utc() > deadline)
                break;
            sched_yield();
        }
    }

DONE:
    ASYNC_TRACE(("Logger loop finished - calling close()\n"));
    internal_close();
    ASYNC_DEBUG_TRACE(("Logger notifying all of exiting (%d) active_files=%d\n",
                       m_thread.use_count(), open_files_count()));

    m_thread.reset();
}

template<typename traits>
void SecdbWriter<traits>::
internal_close() {
    ASYNC_TRACE(("Logger is closing\n"));
    std::unique_lock<std::mutex> lock(m_mutex);
    for (auto* si : m_files)
        internal_close(si, 0);
}

template<typename traits>
void SecdbWriter<traits>::
internal_close(StreamInfo* a_si, int a_errno) {
    if (!a_si || a_si->fd < 0)
        return;

    int fd = a_si->fd;

    assert(fd > 0);

    if (!a_si->pending_queue_empty()) {
        for (Command* p = a_si->m_pending_writes_head, *next; p; p = next) {
            next = p->next;
            deallocate_command(p);
        }
        a_si->m_pending_writes_head = a_si->m_pending_writes_tail = NULL;
    }

    close_event_type_ptr on_close = a_si->on_close;
    ASYNC_TRACE(("----> close(%p, %d) (fd=%d) %s event_val=%d, "
                 "use_count=%ld, active=%ld\n",
            a_si, a_si->error, fd,
            on_close ? "notifying caller" : "will NOT notify caller",
            on_close ? on_close->value() : 0,
            on_close.use_count(), m_active_count.load()));

    // Have to decrement it before resetting the si on the next line
    m_active_count.fetch_sub(1, std::memory_order_relaxed);

    a_si->reset(a_errno);
    m_files[fd] = NULL;
}

template<typename traits>
void SecdbWriter<traits>::
set_formatter(FileID& a_id, msg_formatter a_formatter) {
    BOOST_ASSERT(a_id.stream());
    a_id.stream()->on_format =
        a_formatter ? a_formatter : &StreamInfo::def_on_format;
}

template<typename traits>
void SecdbWriter<traits>::
set_writer(FileID& a_id, msg_writer a_writer) {
    BOOST_ASSERT(a_id.stream());
    a_id.stream()->on_write = a_writer ? a_writer : &writev;
}

template<typename traits>
void SecdbWriter<traits>::
set_error_handler(const err_handler& a_err_handler) {
    m_err_handler = a_err_handler;
}

template<typename traits>
void SecdbWriter<traits>::
set_batch_size(FileID& a_id, size_t a_size) {
    BOOST_ASSERT(a_id.stream());
    a_id.stream()->max_batch_sz = a_size < IOV_MAX ? a_size : IOV_MAX;
}

template<typename traits>
void SecdbWriter<traits>::
set_reconnect(FileID& a_id, stream_reconnecter a_reconnecter) {
    BOOST_ASSERT(a_id.stream());
    a_id.stream()->on_reconnect = a_reconnecter;
}

template<typename traits>
typename SecdbWriter<traits>::FileID
SecdbWriter<traits>::
open_file(const std::string& a_filename, bool a_append, int a_mode)
{
    int n = ::open(a_filename.c_str(),
                a_append ? O_CREAT|O_APPEND|O_WRONLY|O_LARGEFILE
                         : O_CREAT|O_WRONLY|O_TRUNC|O_LARGEFILE,
                a_mode);
    return internal_register_stream(a_filename, &writev, NULL, n);
}

template<typename traits>
typename SecdbWriter<traits>::FileID
SecdbWriter<traits>::
open_file_or_throw(const std::string& a_filename, bool a_append, int a_mode)
{
    int n = ::open(a_filename.c_str(),
                a_append ? O_CREAT|O_APPEND|O_WRONLY|O_LARGEFILE
                         : O_CREAT|O_WRONLY|O_TRUNC|O_LARGEFILE,
                a_mode);
    if (n < 0)
        throw io_error(errno, "Cannot open file '", a_filename,
                       "' for writing");
    return internal_register_stream(a_filename, &writev, NULL, n);
}

template<typename traits>
typename SecdbWriter<traits>::FileID
SecdbWriter<traits>::
open_stream(const std::string&  a_name,
            msg_writer          a_writer,
            stream_state_base*  a_state,
            int                 a_fd)
{
    // Reserve a valid file descriptor by allocating a socket
    int n = a_fd < 0 ? socket(AF_INET, SOCK_DGRAM, 0) : a_fd;
    return internal_register_stream(a_name, a_writer, a_state, n);
}

template<typename traits>
typename SecdbWriter<traits>::FileID
SecdbWriter<traits>::
open_stream_or_throw(const std::string&  a_name,
                     msg_writer          a_writer,
                     stream_state_base*  a_state,
                     int                 a_fd)
{
    // Reserve a valid file descriptor by allocating a socket
    int n = a_fd < 0 ? socket(AF_INET, SOCK_DGRAM, 0) : a_fd;
    if (n < 0)
        throw io_error(errno, "Cannot allocate stream '", a_name, "' socket");
    return internal_register_stream(a_name, a_writer, a_state, n);
}

template<typename traits>
typename SecdbWriter<traits>::FileID
SecdbWriter<traits>::
internal_register_stream(
    const std::string&  a_name,
    msg_writer          a_writer,
    stream_state_base*  a_state,
    int                 a_fd)
{
    if (!check_range(a_fd)) {
        int e = errno;
        if (a_fd > -1) ::close(a_fd);
        errno = e;
        return FileID();
    }

    StreamInfo* si =
        new StreamInfo(this, a_name, a_fd, ++m_last_version, a_writer, a_state);

    internal_update_stream(si, a_fd);

    m_active_count.fetch_add(1, std::memory_order_relaxed);
    return FileID(si);
}

template<typename traits>
bool SecdbWriter<traits>::
internal_update_stream(StreamInfo* a_si, int a_fd) {
    if (!check_range(a_fd))
        return false;

    assert(a_fd > 0);

    std::unique_lock<std::mutex> lock(m_mutex);

    a_si->fd = a_fd;
    a_si->error = 0;
    a_si->error_msg.clear();

    StreamInfo* old_si = m_files[a_fd];
    if (old_si && old_si != a_si) {
        Command* c = allocate_command(Command::destroy_stream, old_si);
        internal_enqueue(c, a_si);
    }

    m_files[a_fd] = a_si;

    return true;
}

template<typename traits>
int SecdbWriter<traits>::
close_file(FileID& a_id, bool a_immediate, int a_wait_secs) {
    if (!a_id) return 0;

    #if defined(DEBUG_ASYNC_LOGGER) && DEBUG_ASYNC_LOGGER != 2
    int fd = a_id.fd();
    #endif

    StreamInfo* si = a_id.stream();

    if (!m_thread) {
        si->reset();
        a_id.reset();
        return 0;
    }

    if (!si->on_close)
        si->on_close.reset(new close_event_type());
    close_event_type_ptr ev = si->on_close;

    long event_val = ev ? ev->value() : 0;

    Command* l_cmd = allocate_command(Command::destroy_stream, a_id.stream());
    l_cmd->args.close.immediate = a_immediate;
    int n = internal_enqueue(l_cmd, a_id.stream());

    if (!n && ev) {
        ASYNC_TRACE(("----> close_file(%d) is waiting for ack secs=%d (event_val={%ld,%d})\n",
                     fd, a_wait_secs, event_val, ev->value()));
        if (m_thread) {
            if (a_wait_secs < 0)
                n = ev->wait(&event_val);
            else {
                auto duration = std::chrono::high_resolution_clock::now();
                auto wait_until = duration + std::chrono::seconds(a_wait_secs);
                n = ev->wait(wait_until, &event_val);
            }
            ASYNC_TRACE(( "====> close_file(%d) ack received (res=%d, val=%ld) (err=%d)\n",
                    fd, n, event_val, si->error));
        }
    } else {
        ASYNC_TRACE(( "====> close_file(%d) failed to enqueue cmd or no event (n=%d, %s)\n",
                fd, n, (ev ? "true" : "false")));
    }
    a_id.reset();
    return n;
}

template<typename traits>
int SecdbWriter<traits>::
internal_enqueue(Command* a_cmd, const StreamInfo* a_si) {
    BOOST_ASSERT(a_cmd);

    Command* old_head;

#ifdef PERF_STATS
    size_t i = 0;
#endif
    // Replace the head with msg
    do {
#ifdef PERF_STATS
        i++;

        if (i > 25)
            sched_yield();
#endif
        old_head = const_cast<Command*>(m_head.load(std::memory_order_relaxed));
        a_cmd->next = old_head;
    } while(!m_head.compare_exchange_weak(old_head, a_cmd,
                std::memory_order_release, std::memory_order_relaxed));

    if (!old_head)
        m_event.signal();

#ifdef PERF_STATS
    if (i > 1) m_stats_enque_spins.fetch_add(i, std::memory_order_relaxed);
#endif

    ASYNC_TRACE(("--> internal_enqueue cmd %p (type=%s) - "
                 "cur head: %p, prev head: %p%s\n",
        a_cmd, a_cmd->type_str(), m_head.load(),
        old_head, !old_head ? " (signaled)" : ""));

    return 0;
}

template<typename traits>
int SecdbWriter<traits>::
internal_write(const FileID& a_id, const std::string& a_category,
               char* a_data, size_t a_sz, bool copied)
{
    if (unlikely(!a_id.stream() || m_cancel)) {
        if (copied)
            deallocate(a_data, a_sz);
        return -1;
    }

    Command* p = allocate_message(a_id.stream(), a_category, a_data, a_sz);
    ASYNC_TRACE(("->write(%p, %lu) - %s\n", a_data, a_sz, copied ? "allocated" : "no copy"));
    return internal_enqueue(p, a_id.stream());
}

template<typename traits>
int SecdbWriter<traits>::
write(const FileID& a_id, const std::string& a_category, void* a_data, size_t a_sz) {
    return internal_write(a_id, a_category, static_cast<char*>(a_data), a_sz, false);
}

template<typename traits>
int SecdbWriter<traits>::
write(const FileID& a_id, const std::string& a_category, const std::string& a_data) {
    char* q = allocate(a_data.size());
    memcpy(q, a_data.c_str(), a_data.size());

    return internal_write(a_id, a_category, q, a_data.size(), false);
}


template<typename traits>
int SecdbWriter<traits>::
do_writev_and_free(StreamInfo* a_si, Command* a_end,
                   const char** a_categories, const iovec* a_vec, size_t a_sz)
{
    int n = a_sz ? a_si->on_write(*a_si, a_categories, a_vec, a_sz) : 0;
    ASYNC_TRACE(("Written %d bytes to stream %s\n", n, a_si->name.c_str()));

    if (likely(n >= 0)) {
        // Data was successfully written to stream - adjust internal queue's head/tail
        a_si->erase(a_si->pending_writes_head(), a_end);
        a_si->pending_writes_head(a_end);
        if (!a_end)
            a_si->pending_writes_tail(a_end);
    } else if (!a_si->error) {
        a_si->set_error(errno);
        if (m_err_handler)
            m_err_handler(*a_si, a_si->error, a_si->error_msg);
        else
            LOG_ERROR("Error writing %lu messages to stream '%s': %s\n",
                       a_sz, a_si->name.c_str(), a_si->error_msg.c_str());
    }
    return n;
}

template<typename traits>
void SecdbWriter<traits>::
deallocate_command(Command* a_cmd) {
    switch (a_cmd->type) {
        case Command::msg:
            deallocate(
                static_cast<char*>(a_cmd->args.msg.data.iov_base),
                a_cmd->args.msg.data.iov_len);

            break;
        default:
            break;
    }
    ASYNC_TRACE(("FD=%d, deallocating command %p (type=%s)\n",
                a_cmd->fd(), a_cmd, a_cmd->type_str()));
    a_cmd->~Command();
    m_cmd_allocator.deallocate(a_cmd, 1);
}

template<typename traits>
int SecdbWriter<traits>::
commit(const struct timespec* tsp)
{
    ASYNC_TRACE(("Committing head: %p\n", m_head.load()));

    int event_val = m_event.value();

    while (!m_cancel && !m_head.load(std::memory_order_relaxed)) {
        #ifdef DEBUG_ASYNC_LOGGER
        wakeup_result n =
        #endif
        m_event.wait(tsp, &event_val);

        ASYNC_DEBUG_TRACE(
            ("  %s COMMIT awakened (res=%s, val=%d, futex=%d), cancel=%d, head=%p\n",
             timestamp::to_string().c_str(), to_string(n), event_val, m_event.value(),
             m_cancel, m_head.load())
        );
    }

    if (m_cancel && !m_head.load(std::memory_order_relaxed))
        return 0;

    Command* cur_head;

#ifdef PERF_STATS
    size_t i = 0;
#endif

    // Find current head and reset the old head to be NULL
    do {
#ifdef PERF_STATS
        i++;
#endif
        cur_head = const_cast<Command*>(m_head.load(std::memory_order_relaxed));
    } while(!m_head.compare_exchange_strong(cur_head, static_cast<Command*>(nullptr),
                std::memory_order_release, std::memory_order_relaxed));

#ifdef PERF_STATS
    if (i > 1) m_stats_deque_spins.fetch_add(i, std::memory_order_relaxed);
#endif
    ASYNC_TRACE((" --> cur head: %p, new head: %p\n", cur_head, m_head.load()));

    BOOST_ASSERT(cur_head);

    int n, count = 0;

    // Place reverse commands in the pending queues of individual streams.
    for(const Command* p = cur_head; p; count += n) {
        StreamInfo* si = const_cast<StreamInfo*>(p->stream);
        BOOST_ASSERT(si);

        #if defined(DEBUG_ASYNC_LOGGER) && DEBUG_ASYNC_LOGGER != 2
        const Command* last = p;
        #endif

        // Insert data to the pending list
        // (this function advances p until there is a stream change)
        n = si->push(p);
        // Update the index of fds that have pending data
        m_pending_data_streams.insert(si);
        ASYNC_TRACE(("Set stream %p fd[%d].pending_writes(%p) -> %d, head(%p), next(%p)\n",
                     si, si->fd, last, n, si->pending_writes_head(), p));
    }

    // Process each fd's pending command queue
    if (m_max_queue_size < count)
        m_max_queue_size = count;

    m_total_msgs_processed.fetch_add(count, std::memory_order_relaxed);

    ASYNC_DEBUG_TRACE(("Processed count: %d / %ld. (MaxQsz = %d)\n",
                       count, m_total_msgs_processed.load(), m_max_queue_size));

    for(typename pending_data_streams_set::iterator
            it = m_pending_data_streams.begin(), e = m_pending_data_streams.end();
            it != e; ++it)
    {
        StreamInfo*     si = *it;
        msg_formatter& ffmt = si->on_format;

        // If there was an error on this stream try to reconnect the stream
        if (si->error && si->on_reconnect) {
            time_val now(time_val::universal_time());
            double time_diff = now.diff(si->last_reconnect_attempt());

            if (time_diff > m_reconnect_sec) {
                ASYNC_TRACE(("===> Trying to reconnect stream %p "
                             "(prev reconnect %.3fs ago)\n",
                             si, si->last_reconnect_attempt() ? time_diff : 0.0));

                int fd = si->on_reconnect(*si);

                ASYNC_TRACE(("     Stream %p %s\n",
                             si, fd < 0 ? "not reconnected!"
                                        : "reconnected successfully!"));

                if (fd >= 0 && !internal_update_stream(si, fd)) {
                    std::string s = to_string("Logger '", si->name.c_str(),
                                              " failed to register file descriptor ",
                                              si->fd, '!');
                    if (m_err_handler)
                        m_err_handler(*si, si->error, s);
                    else
                        LOG_ERROR((s.c_str()));
                }

                si->m_last_reconnect_attempt = now;
            }
        }

        ASYNC_TRACE(("Processing commands for stream %p (fd=%d)\n", si, si->fd));

        struct iovec iov [si->max_batch_sz];  // Contains pointers to write
        const  char* cats[si->max_batch_sz];  // List of message categories
        size_t n = 0, sz = 0;

        static const int SI_OK               = 0;
        static const int SI_CLOSE_SCHEDULED  = 1 << 0;
        static const int SI_CLOSE            = 1 << 1 | SI_CLOSE_SCHEDULED;
        static const int SI_DESTROY          = 1 << 2 | SI_CLOSE;

        int status = SI_OK;

        const Command* p = si->pending_writes_head();
        Command* end;

        // Process commands in blocks of si->max_batch_sz
        for (; p && !si->error && ((status & SI_CLOSE) != SI_CLOSE); p = end) {
            end = p->next;

            if (p->type == Command::msg) {
                iov[n]  = ffmt(p->args.msg.category, p->args.msg.data);
                cats[n] = p->args.msg.category.c_str();
                sz     += iov[n].iov_len;
                ASYNC_TRACE(("FD=%d (stream %p) cmd %p (#%lu) next(%p), "
                             "write(%p, %lu) free(%p, %lu)\n",
                             si->fd, si, p, n, p->next, iov[n].iov_base, iov[n].iov_len,
                             p->args.msg.data.iov_base, p->args.msg.data.iov_len));
                assert(n < si->max_batch_sz);

                if (++n == si->max_batch_sz) {
                    int ec = do_writev_and_free(si, end, cats, iov, n);
                    if (ec > 0)
                        n = 0;
                }
            } else if (p->type == Command::close) {
                status |= p->args.close.immediate ? SI_CLOSE : SI_CLOSE_SCHEDULED;
                ASYNC_TRACE(("FD=%d, Command %lu address %p (close)\n", si->fd, n, p));
                si->erase(const_cast<Command*>(p));
            } else if (p->type == Command::destroy_stream) {
                status |= SI_DESTROY;
                si->erase(const_cast<Command*>(p));
            } else {
                ASYNC_TRACE(("Command %p has invalid message type: %s "
                             "(stream=%p, prev=%p, next=%p)\n",
                             p, p->type_str(), p->stream, p->prev, p->next));
                si->erase(const_cast<Command*>(p));
                BOOST_ASSERT(false); // This should never happen!
            }
        }

        if (si->error) {
            ASYNC_TRACE(("Written total %lu bytes to %p (fd=%d) %s with error: %s\n",
                         sz, si, si->fd, si->name.c_str(), si->error_msg.c_str()));
        } else {
            if (n > 0)
                do_writev_and_free(si, end, cats, iov, n);

            ASYNC_TRACE(("Written total %lu bytes to (fd=%d) %s\n",
                         sz, si->fd, si->name.c_str()));
        }

        // Close associated file descriptor
        if (si->error || status != SI_OK) {
            bool destroy_si = (status & SI_DESTROY);

            if (destroy_si || si->fd < 0) {
                ASYNC_DEBUG_TRACE(("Removing %p stream from list of pending data streams\n", si));
                m_pending_data_streams.erase(si);
            }

            internal_close(si, si->error);

            if (destroy_si) {
                ASYNC_TRACE(("<<< Destroying %p stream\n", si));
                delete si;
            }
        }
    }
    return count;
}

} // namespace secdb
