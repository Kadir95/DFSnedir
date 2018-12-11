FROM python:3
ENV TESTPORT=19199
ENV PORT=11223
ENV INITSH=dev_init
EXPOSE ${TESTPORT}
EXPOSE ${PORT}
ADD server/ /App/
RUN ./App/container_scripts/${INITSH}.sh
CMD [ "python", "/App/idle.py" ]