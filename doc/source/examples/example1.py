def pass_input(inbox):
    input = inbox[0]
    return input

def merge_inputs(inbox):
    input1 = inbox[0]
    input2 = inbox[1]
    return (input1, input2)

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
    
multiply_by_2 = Worker(multiply_by, number =2)         
calculate_sum = Worker(calculator, operation ='sum')

multiply_by_3 = Worker(multiply_by, 3)
calculate_product = Worker(calculator, 'multiply')

# positional arguments
sum_and_multiply_by_2 = Worker((calculator, multiply_by), (('sum',),   (3,)))
# keyworded arguments
sum_and_multiply_by_2 = Worker((calculator, multiply_by), kwargs =( \
                                                            {'operation':'sum'}, 
                                                            {'number':3}))