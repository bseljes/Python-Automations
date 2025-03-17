import turtle
import random

'''
This is a fun little program I made to show a coworker what the turtle package is.
'''
turtle.setup(width=500, height=500)

class Food(turtle.Turtle):
    def __init__(self):
        super().__init__()
        self.penup()
        self.colors = ['blue', 'black', 'red', 'green', 'yellow', 'orange', 'purple']
        self.col_num = random.randint(0, len(self.colors) - 1)
        self.color(self.colors[self.col_num])
        self.x = random.randint(-12, 12) * 20
        self.y = random.randint(-12, 12) * 20
        self.goto(self.x, self.y)

    def move(self):
        self.hideturtle()
        self.x = random.randint(-12, 12) * 20
        self.y = random.randint(-12, 12) * 20
        self.goto(self.x, self.y)
        self.col_num = random.randint(0, 3)
        self.color(self.colors[self.col_num])
        self.showturtle()

# Function to move the turtle up
def move_up():
    y = tim.ycor()
    if tim.heading() != 90:
        tim.setheading(90)
    tim.sety(y + 20)
    check_collision()

# Function to move the turtle down
def move_down():
    y = tim.ycor()
    if tim.heading() != 270:
        tim.setheading(270)
    tim.sety(y - 20)
    check_collision()

# Function to move the turtle left
def move_left():
    x = tim.xcor()
    if tim.heading() != 180:
        tim.setheading(180)
    tim.setx(x - 20)
    check_collision()

# Function to move the turtle right
def move_right():
    x = tim.xcor()
    if tim.heading() != 0:
        tim.setheading(0)
    tim.setx(x + 20)
    check_collision()

# Check for collision between turtle and food
def check_collision():
    if tim.distance(one_food) < 10:
        tim.color(one_food.colors[one_food.col_num])
        one_food.move()

# Initialize the turtle
tim = turtle.Turtle()
tim.speed(10)
tim.shape("turtle")
tim.color("blue")
tim.penup()
tim.goto(0, 0)

# Create a food object
one_food = Food()

# Set up the key binding
turtle.onkey(move_up, "w")
turtle.onkey(move_down, "s")
turtle.onkey(move_left, "a")
turtle.onkey(move_right, "d")
turtle.listen()

# Main loop
turtle.mainloop()
