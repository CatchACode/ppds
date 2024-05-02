//
// Created by CatchACode on 02.05.24.
//

#ifndef LOGGER_H
#define LOGGER_H

#include <queue>
#include <mutex>
#include <iostream>

class Logger {
    public:
    /**
     *
     * @param out ostream to output to
     * @param fmtMsg std::format'ed string to output, can also just be a normal string
     */
    static void log(const std::ostream& out, const std::string& fmtMsg) {
        {
            std::lock_guard<std::mutex> lock(m_cout);
            out << fmtMsg << std::endl;
        }
    }

    private:
    //static std::queue<std::string> msgQueue;
    //static std::mutex m_msgQueue;
    static std::mutex m_cout;
};

#endif //LOGGER_H
