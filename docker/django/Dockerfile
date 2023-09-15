FROM python:3.8

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Create and set the working directory
WORKDIR /app

RUN pip install django

# Expose port
EXPOSE 8000

# Copy your Django code into the container
COPY cloudsource /app/

# Set the entry point
CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]
