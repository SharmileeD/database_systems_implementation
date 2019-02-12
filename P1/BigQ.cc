#include "BigQ.h"
#include "Record.h"
#include "Schema.h"
#include "Pipe.h"
BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
        // read data from in pipe sort them into runlen pages

    // construct priority queue over sorted runs and dump sorted data
        // into the out pipe

    // finally shut down the out pipe
        Record rec[2];
        Record *last = NULL, *prev = NULL;
        int err = 0;
        int i = 0;
        Schema mySchema ("catalog", "nation");
        while (in.Remove(&rec[i%2])) {
                prev = last;
                last = &rec[i%2];
                last->Print(&mySchema);
                i++;
        }
        cout << "inside Bigq"<<endl;
        out.ShutDown ();
}

BigQ::~BigQ () {
}