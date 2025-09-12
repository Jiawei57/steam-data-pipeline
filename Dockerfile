# Use an official Python runtime as a parent image
FROM python:3.11-slim-bullseye AS base

# Set environment variables to prevent Python from writing .pyc files to disc
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Set the working directory in the container
WORKDIR /app

# Create a non-root user to run the application for better security
RUN useradd --system --uid 1000 --gid 0 -m -s /bin/bash appuser

# --- Dependencies Stage ---
FROM base AS dependencies

# Copy the requirements file
COPY --chown=appuser:0 requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# --- Application Stage ---
FROM base AS application
COPY --from=dependencies /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages

# Copy the current directory contents into the container at /app
COPY --chown=appuser:0 *.py ./

# Switch to the non-root user
USER appuser

# Command to run the application (the background worker)
CMD ["python", "runner.py"]