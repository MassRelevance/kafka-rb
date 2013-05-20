# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
module Kafka
  module IO
    attr_accessor :socket, :host, :port, :compression

    HOST = "localhost"
    PORT = 9092

    def connect(host, port)
      raise ArgumentError, "No host or port specified" unless host && port
      self.host = host
      self.port = port
      self.socket = TCPSocket.new(host, port)
    end

    def reconnect(opts={})
      Timeout::timeout(opts[:timeout]) do
        self.socket = TCPSocket.new(self.host, self.port)
      end
    rescue
      self.disconnect(opts)
      raise
    end

    def disconnect(opts={})
      Timeout::timeout(opts[:timeout]) do
        self.socket.close rescue nil
        self.socket = nil
      end
    end

    def read(length, opts={})
      Timeout::timeout(opts[:timeout]) do
        self.socket.read(length) || raise(SocketError, "no data")
      end
    rescue
      self.disconnect(opts)
      raise
    end

    def write(data, opts={})
      Timeout::timeout(opts[:timeout]) do
        self.reconnect(opts) unless self.socket
        self.socket.write(data)
      end
    rescue
      self.disconnect(opts)
      raise
    end

  end
end
