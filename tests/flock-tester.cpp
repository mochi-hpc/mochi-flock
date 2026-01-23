/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */

/*
 * Interactive Flock Backend Tester
 *
 * An interactive command-line program for manually testing Flock backends.
 * Manages multiple worker processes that each run a Flock provider.
 *
 * Usage:
 *     flock-tester <config_file>
 *
 * Commands:
 *     help        - List commands and their effects
 *     start       - Spawn a new process and make it join the group
 *     stop <N>    - Gracefully stop process N (sends LEAVE announcement)
 *     kill <N>    - Kill process N without cleanup (simulates crash)
 *     view        - Update and display current group view
 *     list        - List all worker processes and their status
 *     sleep <N>   - Sleep for N seconds (useful for scripting)
 *     exit        - Kill all processes and exit
 *
 * Configuration file format (JSON):
 *     {
 *         "transport": "na+sm",
 *         "group": {
 *             "type": "swim",
 *             "config": {
 *                 "protocol_period_ms": 500.0,
 *                 "ping_timeout_ms": 100.0,
 *                 "suspicion_timeout_ms": 2000.0
 *             }
 *         }
 *     }
 */

#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <map>
#include <memory>
#include <cstdlib>
#include <cstring>
#include <csignal>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <fcntl.h>

#include <json-c/json.h>
#include <margo.h>
#include <flock/flock-client.h>
#include <flock/flock-group.h>

// Worker status
enum class WorkerStatus {
    Running,
    Stopped,
    Killed
};

// Worker information
struct Worker {
    int id;
    pid_t pid;
    std::string address;
    std::string control_fifo;
    WorkerStatus status;
};

class FlockTester {
public:
    FlockTester(const std::string& config_file);
    ~FlockTester();

    void run();

private:
    void loadConfig(const std::string& config_file);
    void cleanup();

    void cmdHelp(const std::vector<std::string>& args);
    void cmdStart(const std::vector<std::string>& args);
    void cmdStop(const std::vector<std::string>& args);
    void cmdKill(const std::vector<std::string>& args);
    void cmdView(const std::vector<std::string>& args);
    void cmdList(const std::vector<std::string>& args);
    void cmdSleep(const std::vector<std::string>& args);
    void cmdExit(const std::vector<std::string>& args);

    std::string findWorkerPath();
    std::vector<std::string> splitLine(const std::string& line);

    std::string m_transport;
    std::string m_provider_config;
    std::string m_temp_dir;
    std::string m_group_file;
    std::string m_worker_path;

    margo_instance_id m_mid = MARGO_INSTANCE_NULL;
    flock_client_t m_client = FLOCK_CLIENT_NULL;

    std::map<int, Worker> m_workers;
    int m_next_id = 0;
    bool m_running = true;
};

FlockTester::FlockTester(const std::string& config_file) {
    loadConfig(config_file);

    // Create temp directory
    char temp_template[] = "/tmp/flock-tester-XXXXXX";
    char* temp_dir = mkdtemp(temp_template);
    if (!temp_dir) {
        throw std::runtime_error("Failed to create temp directory");
    }
    m_temp_dir = temp_dir;
    m_group_file = m_temp_dir + "/group.json";

    // Find worker executable path
    m_worker_path = findWorkerPath();

    // Create Margo engine for client
    m_mid = margo_init(m_transport.c_str(), MARGO_CLIENT_MODE, 0, 0);
    if (m_mid == MARGO_INSTANCE_NULL) {
        rmdir(m_temp_dir.c_str());
        throw std::runtime_error("Failed to initialize Margo");
    }

    // Create Flock client
    flock_return_t ret = flock_client_init(m_mid, ABT_POOL_NULL, &m_client);
    if (ret != FLOCK_SUCCESS) {
        margo_finalize(m_mid);
        rmdir(m_temp_dir.c_str());
        throw std::runtime_error("Failed to initialize Flock client");
    }
}

FlockTester::~FlockTester() {
    cleanup();
}

void FlockTester::loadConfig(const std::string& config_file) {
    std::ifstream file(config_file);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open config file: " + config_file);
    }

    std::stringstream buffer;
    buffer << file.rdbuf();
    std::string config_str = buffer.str();

    json_object* config = json_tokener_parse(config_str.c_str());
    if (!config) {
        throw std::runtime_error("Failed to parse config file");
    }

    // Get transport
    json_object* transport_obj;
    if (json_object_object_get_ex(config, "transport", &transport_obj)) {
        m_transport = json_object_get_string(transport_obj);
    } else {
        m_transport = "na+sm";
    }

    // Get group config and build provider config
    json_object* group_obj;
    if (json_object_object_get_ex(config, "group", &group_obj)) {
        json_object* provider_config = json_object_new_object();
        json_object_object_add(provider_config, "group", json_object_get(group_obj));
        m_provider_config = json_object_to_json_string(provider_config);
        json_object_put(provider_config);
    } else {
        m_provider_config = R"({"group":{"type":"static","config":{}}})";
    }

    json_object_put(config);
}

std::string FlockTester::findWorkerPath() {
    // Try to find flock-worker in the same directory as this executable
    char path[1024];
    ssize_t len = readlink("/proc/self/exe", path, sizeof(path) - 1);
    if (len != -1) {
        path[len] = '\0';
        std::string exe_path(path);
        size_t pos = exe_path.rfind('/');
        if (pos != std::string::npos) {
            std::string worker_path = exe_path.substr(0, pos + 1) + "flock-worker";
            if (access(worker_path.c_str(), X_OK) == 0) {
                return worker_path;
            }
        }
    }

    // Try current directory
    if (access("./flock-worker", X_OK) == 0) {
        return "./flock-worker";
    }

    throw std::runtime_error("Could not find flock-worker executable");
}

void FlockTester::cleanup() {
    // Kill all workers
    for (auto& [id, worker] : m_workers) {
        if (worker.status == WorkerStatus::Running) {
            kill(worker.pid, SIGKILL);
            waitpid(worker.pid, nullptr, 0);
        }
        // Remove FIFO
        unlink(worker.control_fifo.c_str());
    }
    m_workers.clear();

    // Cleanup client and engine
    if (m_client != FLOCK_CLIENT_NULL) {
        flock_client_finalize(m_client);
        m_client = FLOCK_CLIENT_NULL;
    }

    if (m_mid != MARGO_INSTANCE_NULL) {
        margo_finalize(m_mid);
        m_mid = MARGO_INSTANCE_NULL;
    }

    // Remove group file and temp directory
    unlink(m_group_file.c_str());
    rmdir(m_temp_dir.c_str());
}

void FlockTester::cmdHelp(const std::vector<std::string>& args) {
    (void)args;
    std::cout << R"(
Commands:
  help        - Show this help message
  start       - Spawn a new process and make it join the group
  stop <N>    - Gracefully stop process N (sends LEAVE announcement)
  kill <N>    - Kill process N without cleanup (simulates crash)
  view        - Update and display current group view
  list        - List all worker processes and their status
  sleep <N>   - Sleep for N seconds (useful for scripting)
  exit        - Kill all processes and exit

Actions:
  stop (graceful) - Provider sends LEAVE announcement, detected quickly
  kill (crash)    - SIGKILL, no cleanup, requires suspicion timeout to detect
)" << std::endl;
}

void FlockTester::cmdStart(const std::vector<std::string>& args) {
    (void)args;

    int worker_id = m_next_id++;

    // Create FIFO for this worker
    std::string fifo_path = m_temp_dir + "/worker-" + std::to_string(worker_id) + ".fifo";
    if (mkfifo(fifo_path.c_str(), 0600) != 0) {
        std::cerr << "Error: Failed to create FIFO" << std::endl;
        return;
    }

    // Create pipes for reading worker's stdout
    int pipefd[2];
    if (pipe(pipefd) != 0) {
        std::cerr << "Error: Failed to create pipe" << std::endl;
        unlink(fifo_path.c_str());
        return;
    }

    pid_t pid = fork();
    if (pid < 0) {
        std::cerr << "Error: Failed to fork" << std::endl;
        close(pipefd[0]);
        close(pipefd[1]);
        unlink(fifo_path.c_str());
        return;
    }

    if (pid == 0) {
        // Child process
        close(pipefd[0]);  // Close read end

        // Redirect stdout to pipe
        dup2(pipefd[1], STDOUT_FILENO);
        close(pipefd[1]);

        // Execute worker
        execl(m_worker_path.c_str(), "flock-worker",
              m_transport.c_str(),
              m_provider_config.c_str(),
              m_group_file.c_str(),
              fifo_path.c_str(),
              nullptr);

        // If execl returns, there was an error
        std::cerr << "Error: Failed to exec worker" << std::endl;
        _exit(1);
    }

    // Parent process
    close(pipefd[1]);  // Close write end

    // Read address from worker's stdout
    char address[256];
    ssize_t n = 0;
    size_t total = 0;
    while (total < sizeof(address) - 1) {
        n = read(pipefd[0], address + total, 1);
        if (n <= 0) break;
        if (address[total] == '\n') {
            address[total] = '\0';
            break;
        }
        total++;
    }
    close(pipefd[0]);

    if (total == 0) {
        std::cerr << "Error: Failed to read worker address" << std::endl;
        kill(pid, SIGKILL);
        waitpid(pid, nullptr, 0);
        unlink(fifo_path.c_str());
        return;
    }

    Worker worker;
    worker.id = worker_id;
    worker.pid = pid;
    worker.address = address;
    worker.control_fifo = fifo_path;
    worker.status = WorkerStatus::Running;

    m_workers[worker_id] = worker;
    std::cout << "Started process " << worker_id << " at " << address << std::endl;
}

void FlockTester::cmdStop(const std::vector<std::string>& args) {
    if (args.empty()) {
        std::cerr << "Usage: stop <N>" << std::endl;
        return;
    }

    int worker_id;
    try {
        worker_id = std::stoi(args[0]);
    } catch (...) {
        std::cerr << "Error: Invalid worker ID: " << args[0] << std::endl;
        return;
    }

    auto it = m_workers.find(worker_id);
    if (it == m_workers.end()) {
        std::cerr << "Error: No worker with ID " << worker_id << std::endl;
        return;
    }

    Worker& worker = it->second;
    if (worker.status != WorkerStatus::Running) {
        std::cerr << "Error: Worker " << worker_id << " is not running" << std::endl;
        return;
    }

    // Send STOP command via FIFO
    int fd = open(worker.control_fifo.c_str(), O_WRONLY | O_NONBLOCK);
    if (fd < 0) {
        std::cerr << "Error: Failed to open control FIFO" << std::endl;
        return;
    }

    const char* cmd = "STOP\n";
    ssize_t written = write(fd, cmd, strlen(cmd));
    (void)written;  // Ignore return value - best effort
    close(fd);

    // Wait for graceful exit
    int status;
    pid_t result = waitpid(worker.pid, &status, 0);
    if (result > 0) {
        worker.status = WorkerStatus::Stopped;
        std::cout << "Stopped process " << worker_id << " gracefully" << std::endl;
    } else {
        std::cerr << "Warning: Failed to wait for worker" << std::endl;
    }
}

void FlockTester::cmdKill(const std::vector<std::string>& args) {
    if (args.empty()) {
        std::cerr << "Usage: kill <N>" << std::endl;
        return;
    }

    int worker_id;
    try {
        worker_id = std::stoi(args[0]);
    } catch (...) {
        std::cerr << "Error: Invalid worker ID: " << args[0] << std::endl;
        return;
    }

    auto it = m_workers.find(worker_id);
    if (it == m_workers.end()) {
        std::cerr << "Error: No worker with ID " << worker_id << std::endl;
        return;
    }

    Worker& worker = it->second;
    if (worker.status != WorkerStatus::Running) {
        std::cerr << "Error: Worker " << worker_id << " is not running" << std::endl;
        return;
    }

    // Send SIGKILL (no cleanup)
    kill(worker.pid, SIGKILL);
    waitpid(worker.pid, nullptr, 0);
    worker.status = WorkerStatus::Killed;
    std::cout << "Killed process " << worker_id << " (crash simulation)" << std::endl;
}

void FlockTester::cmdView(const std::vector<std::string>& args) {
    (void)args;

    // Find a running worker to query
    Worker* running_worker = nullptr;
    for (auto& [id, worker] : m_workers) {
        if (worker.status == WorkerStatus::Running) {
            running_worker = &worker;
            break;
        }
    }

    if (!running_worker) {
        std::cerr << "Error: No running workers to query" << std::endl;
        return;
    }

    // Lookup address
    hg_addr_t addr;
    hg_return_t hret = margo_addr_lookup(m_mid, running_worker->address.c_str(), &addr);
    if (hret != HG_SUCCESS) {
        std::cerr << "Error: Failed to lookup worker address" << std::endl;
        return;
    }

    // Create group handle
    flock_group_handle_t gh;
    flock_return_t ret = flock_group_handle_create(m_client, addr, 0, FLOCK_MODE_INIT_UPDATE, &gh);
    margo_addr_free(m_mid, addr);

    if (ret != FLOCK_SUCCESS) {
        std::cerr << "Error: Failed to create group handle" << std::endl;
        return;
    }

    // Update view
    ret = flock_group_update_view(gh, nullptr);
    if (ret != FLOCK_SUCCESS) {
        std::cerr << "Error: Failed to update view" << std::endl;
        flock_group_handle_release(gh);
        return;
    }

    // Get view
    flock_group_view_t view = FLOCK_GROUP_VIEW_INITIALIZER;
    ret = flock_group_get_view(gh, &view);
    if (ret != FLOCK_SUCCESS) {
        std::cerr << "Error: Failed to get view" << std::endl;
        flock_group_handle_release(gh);
        return;
    }

    // Display view
    size_t member_count = flock_group_view_member_count(&view);
    std::cout << "\nGroup View (" << member_count << " member"
              << (member_count != 1 ? "s" : "") << "):" << std::endl;

    for (size_t i = 0; i < member_count; i++) {
        flock_member_t* member = flock_group_view_member_at(&view, i);
        std::cout << " - " << member->address
                  << " (provider_id=" << member->provider_id << ")" << std::endl;
    }

    // Display metadata (skip internal)
    size_t metadata_count = flock_group_view_metadata_count(&view);
    bool has_user_metadata = false;
    for (size_t i = 0; i < metadata_count; i++) {
        flock_metadata_t* md = flock_group_view_metadata_at(&view, i);
        if (strncmp(md->key, "__", 2) != 0) {
            if (!has_user_metadata) {
                std::cout << "\nMetadata:" << std::endl;
                has_user_metadata = true;
            }
            std::cout << "  " << md->key << ": " << md->value << std::endl;
        }
    }

    std::cout << std::endl;

    flock_group_view_clear(&view);
    flock_group_handle_release(gh);
}

void FlockTester::cmdList(const std::vector<std::string>& args) {
    (void)args;

    if (m_workers.empty()) {
        std::cout << "No workers" << std::endl;
        return;
    }

    std::cout << "\nWorkers:" << std::endl;
    for (const auto& [id, worker] : m_workers) {
        const char* status_str = "unknown";
        switch (worker.status) {
            case WorkerStatus::Running: status_str = "running"; break;
            case WorkerStatus::Stopped: status_str = "stopped"; break;
            case WorkerStatus::Killed:  status_str = "killed";  break;
        }
        std::cout << "  [" << id << "] " << worker.address
                  << " - " << status_str << std::endl;
    }
    std::cout << std::endl;
}

void FlockTester::cmdSleep(const std::vector<std::string>& args) {
    if (args.empty()) {
        std::cerr << "Usage: sleep <N>" << std::endl;
        return;
    }

    int seconds;
    try {
        seconds = std::stoi(args[0]);
    } catch (...) {
        std::cerr << "Error: Invalid number of seconds: " << args[0] << std::endl;
        return;
    }

    sleep(seconds);
}

void FlockTester::cmdExit(const std::vector<std::string>& args) {
    (void)args;
    std::cout << "Cleaning up..." << std::endl;
    m_running = false;
}

std::vector<std::string> FlockTester::splitLine(const std::string& line) {
    std::vector<std::string> tokens;
    std::istringstream iss(line);
    std::string token;
    while (iss >> token) {
        tokens.push_back(token);
    }
    return tokens;
}

void FlockTester::run() {
    std::cout << "Flock Backend Tester" << std::endl;
    std::cout << "Type 'help' for commands.\n" << std::endl;

    std::string line;
    while (m_running) {
        std::cout << "> ";
        std::cout.flush();

        if (!std::getline(std::cin, line)) {
            break;
        }

        auto tokens = splitLine(line);
        if (tokens.empty()) {
            continue;
        }

        std::string cmd = tokens[0];
        std::vector<std::string> args(tokens.begin() + 1, tokens.end());

        // Convert to lowercase
        for (char& c : cmd) {
            c = std::tolower(c);
        }

        if (cmd == "help") {
            cmdHelp(args);
        } else if (cmd == "start") {
            cmdStart(args);
        } else if (cmd == "stop") {
            cmdStop(args);
        } else if (cmd == "kill") {
            cmdKill(args);
        } else if (cmd == "view") {
            cmdView(args);
        } else if (cmd == "list") {
            cmdList(args);
        } else if (cmd == "sleep") {
            cmdSleep(args);
        } else if (cmd == "exit" || cmd == "quit") {
            cmdExit(args);
        } else {
            std::cerr << "Unknown command: " << cmd << std::endl;
            std::cerr << "Type 'help' for available commands." << std::endl;
        }
    }
}

int main(int argc, char** argv)
{
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <config_file>" << std::endl;
        std::cerr << "\nConfig file format (JSON):" << std::endl;
        std::cerr << R"(  {
    "transport": "na+sm",
    "group": {
      "type": "swim",
      "config": {
        "protocol_period_ms": 500.0,
        "ping_timeout_ms": 100.0,
        "suspicion_timeout_ms": 2000.0
      }
    }
  })" << std::endl;
        return 1;
    }

    try {
        FlockTester tester(argv[1]);
        tester.run();
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
