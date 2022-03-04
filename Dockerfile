FROM arpasmr/r-base 
COPY . /usr/local/src/myscripts
WORKDIR /usr/local/src/myscripts
RUN chmod +x anagrafica_LIRIS.sh
CMD ["./anagrafica_LIRIS.sh"]
