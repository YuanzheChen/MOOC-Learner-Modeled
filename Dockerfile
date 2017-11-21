FROM modeled:base
RUN mkdir app
ADD . /app/MOOC-Learner-Modeled
WORKDIR /app/MOOC-Learner-Modeled
RUN ["chmod", "+x", "wait_for_it.sh"]
CMD ["./wait_for_it.sh", "quantified", "python", "-u", "autorun.py", "-c", "../config/config.yml"]
