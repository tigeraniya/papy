Examples
========

    1. Writing a worker-function.
  
        We will start by writing the simples possible function it will do 
        nothing but return the input::
  
            def pass_input(inbox):
                input = inbox[0]
                return input
                
        This function can only be used in a *Piper*, which has only one input
        i.e. it uses only the first element of the inbox tuple. A similar 
        function could be used at a branching point.::
        
            def merge_inputs(inbox):
                input1 = inbox[0]
                input2 = inbox[1]
                return (input1, input2)        

        Writing a worker-function which takes arguments.
        
        A function can be written to take arguments (with or without default 
        values). In the first function the first element in the inbox will be 
        multiplied by a constant factor specified as the number argument. In the
        second example an arithmetical operation will be done on two numbers
        as given by the operation argument.::
        
            def multiply_by(inbox, number):
                input = inbox[0]
                output = input * number
                return output
                
            def calculator(inbox, operation):
                input1 = inbox[0]
                input2 = inbox[1]
                if operation == 'add':
                    result = input1 + input2
                elif operation == 'subtract':
                    result = input1 - input2
                elif operation == 'multiply':
                    result = input1 * input2
                elif operation == 'divide':
                    result = input1 / input2
                else:
                    result = 'error'
                return result
                        
        These the arguments in these functions are specified when a *Worker* is
        constructed::
        
            multiply_by_2 = Worker(multiply_by, number =2)         
            calculate_sum = Worker(calculator, operation ='sum')
        
        The argument names need not to be given::
        
            multiply_by_3 = Worker(multiply_by, 3)
            calculate_product = Worker(calculator, 'multiply')
        
        If a worker is constructed from multiple functions i.e. "sum the two 
        inputs and multiply the result by 2"::
        
            # positional arguments
            sum_and_multiply_by_2 = Worker((calculator, multiply_by), \
                                           (('sum',),   (3,))):
            # keyworded arguments
            sum_and_multiply_by_2 = Worker((calculator, multiply_by), \
                                            kwargs = \
                                           ({'operation':'sum'}, {'number':3}))
        
        In the last example the second argument given is in the first version a 
        tuple of tuples which are the positional arguments for the function or
        as in the second example a tuple of dictionaries with named arguments.