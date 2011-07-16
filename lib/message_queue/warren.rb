module MessageQueue
  class Warren
    class IncompleteOperation < StandardError; end
    class NecromancyRequired  < StandardError; end


    def initialize(opts={})
      @rabbits        = []
      @enqueue_cursor = 0
      @dequeue_cursor = 0
      @confirm_cursor = nil

      if not opts['warren'] and not opts['warren'].is_a?(Array)
        raise ArgumentError, "Your configuration is missing a warren of rabbits!"
      else
        opts['warren'].each do |rabbit_opts|
          @rabbits << FuzzyRabbit.new(rabbit_opts)
        end
      end
    end


    def queue_size(queue_name)
      queue_size = 0

      self.each do |rabbit|
        begin
          queue_size += rabbit.queue_size(queue_name)
        rescue MessageQueue::FuzzyRabbit::MarkedDead 
        end
      end

      return queue_size
    end

 
    def queue_sizes(queue_name)
      queue_sizes = {}

      self.each do |rabbit| 
        begin
          queue_sizes[rabbit.to_key] = rabbit.queue_size(queue_name)
        rescue MessageQueue::FuzzyRabbit::MarkedDead 
          queue_sizes[rabbit.to_key] = nil
        end
      end

      return queue_sizes
    end


    def delete
      self.each do |rabbit|
        rabbit.delete
      end
    end


    def stop
      self.each do |rabbit|
        rabbit.stop 
      end
    end


    def each(&block)
      errors_on = [] 
      @rabbits.each do |rabbit|
        begin
          block.call(rabbit)
        rescue MessageQueue::FuzzyRabbit::MarkedDead
          errors_on << rabbit.to_s
        end
      end

      if errors_on.any?
        msg = "operation not run on: #{ errors_on.join(',') }"
        raise IncompleteOperation, msg
      end
    end


    ## HACK: to support fanout exchanges until the Java/Scala clients catch up
    def client
      return @rabbits.first.client
    end

    def clients
      return @rabbits
    end

    def confirm(*args)
      return nil unless @confirm_cursor 
      rabbit          = @rabbits[@confirm_cursor]
      ret             = rabbit.confirm(*args)
      @confirm_cursor = nil 
      return ret
    end


    ## dequeue - will rotate and dequeue until (a) all queues have been checked
    ##           or (b) a message is dequeued
    def dequeue(*args)
      errors = 0
      ret    = nil

      @rabbits.size.times do 
        begin
          rabbit = @rabbits[move_dequeue_cursor]
          if ret = rabbit.dequeue(*args)
            @confirm_cursor = @dequeue_cursor 
            break
          end
        rescue MessageQueue::FuzzyRabbit::MarkedDead
          errors += 1
        end
      end

      if errors == @rabbits.size 
        ## puts "#{self.class}#dequeue: Raising NecromancyRequired -- All Rabbits Are Dead!"
        raise MessageQueue::Warren::NecromancyRequired, "All Rabbits are dead"
      end

      return ret 
    end


    def enqueue(*args)
      each_until_write do |rabbit|
        rabbit.enqueue(*args)
      end
    end


    def publish_to_fanout(*args)
      each_until_write do |rabbit|
        rabbit.publish_to_fanout(*args)
      end
    end

    ##  enqueue - will rotate and enqueue
    def each_until_write(&block)
      errors = 0
      ret    = nil 

      @rabbits.size.times do 
        begin
          rabbit = @rabbits[move_enqueue_cursor]
          ret    = block.call(rabbit)
          break 
        rescue MessageQueue::FuzzyRabbit::MarkedDead
          errors += 1
        end
      end

      if errors == @rabbits.size 
        ## puts "#{self.class}#each_until_write: Raising NecromancyRequired -- All Rabbits Are Dead!"
        raise MessageQueue::Warren::NecromancyRequired, "All Rabbits are dead"
      end

      return ret
    end 



    private

    def move_enqueue_cursor
      if @rabbits.size - 1 > @enqueue_cursor
        @enqueue_cursor += 1
      else
        @enqueue_cursor  = 0
      end
      return @enqueue_cursor
    end

    def move_dequeue_cursor
      if @rabbits.size - 1 > @dequeue_cursor
        @dequeue_cursor += 1
      else
        @dequeue_cursor  = 0
      end
      return @dequeue_cursor
    end

  end
end
