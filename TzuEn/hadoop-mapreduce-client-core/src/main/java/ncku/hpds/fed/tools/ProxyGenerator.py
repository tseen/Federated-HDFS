import sys
import getopt

def readfile():
    ins = open( "file.txt", "r" )
    array = []
    for line in ins:
        array.append( line )
    ins.close()

def write_add_proxy_mappers(out,typeClasz):
    out.write("\tprivate void addProxyMappers(){\n")
    for out_line in typeClasz:
        for inner_line in typeClasz:
            tmp = "\t\taddProxyMapperMapping( %s.class, %s.class, %s.class );\n" % (out_line, inner_line, "ProxyMapper"+out_line.replace("Writable","")+inner_line.replace("Writable",""))
            out.write(tmp)
    out.write("\t}\n\n")

def write_add_proxy_reducers(out,typeClasz):
    out.write("\tprivate void addProxyReducers(){\n")
    for out_line in typeClasz:
        for inner_line in typeClasz:
            tmp = "\t\taddProxyReducerMapping( %s.class, %s.class, ProxyReducer%s%s.class );\n" % (out_line, inner_line, out_line.replace("Writable",""),inner_line.replace("Writable",""))
            out.write(tmp)
    out.write("\t}\n")

def write_proxy_mappers(out,typeClasz):
    out.write("\t//----------------------------------------------------------\n")
    out.write("\t// Proxy Mapper Dummay Classes \n\n")
    for out_line in typeClasz:
        out.write("\t// Proxy Mapper " + out_line +"\n" )
        for inner_line in typeClasz:
            tmp = "\tpublic static class ProxyMapper%s%s extends GenericProxyMapper <%s,%s>{\n" % (out_line.replace("Writable",""),inner_line.replace("Writable",""),out_line,inner_line)
            out.write(tmp)
            tmp = "\t\tpublic ProxyMapper%s%s() throws Exception { super(%s.class,%s.class); }\n" % (out_line.replace("Writable",""),inner_line.replace("Writable",""),out_line,inner_line)
            out.write(tmp)
            out.write("\t}\n")
        out.write("\n")

def write_proxy_reducers(out,typeClasz):
    out.write("\t//----------------------------------------------------------\n")
    out.write("\t// Proxy Reducer Dummay Classes \n\n")
    for out_line in typeClasz:
        out.write("\t// Proxy Reducer " + out_line +"\n" )
        for inner_line in typeClasz:
            tmp = "\tpublic static class ProxyReducer%s%s extends GenericProxyReducer <%s, %s>{}\n" % (out_line.replace("Writable",""),inner_line.replace("Writable",""),out_line,inner_line)
            out.write(tmp)
        out.write("\n")

def write_partial_generic_proxy_mapper_keypart(out,typeClasz):
    i = 0
    size = len(typeClasz)
    out.write("\t//----------------------------------------------------------\n")
    out.write("\t// Partial Generic Mapper Keypart \n")
    for line in typeClasz:
        prim = line.replace("Writable","")
        setStr = ""
        if prim == "Text" or prim == "UTF8":
            setStr = "\t\ttKey.set(keyPart)\n"
        elif prim == "Int":
            setStr = "\t\ttKey.set(Integer.valueOf(keyPart));\n"
        elif prim == "VInt":
            setStr = "\t\ttKey.set(Integer.valueOf(keyPart));\n"
        elif prim == "VLong":
            setStr = "\t\ttKey.set(Long.valueOf(keyPart));\n"
        else:
            setStr = "\t\ttKey.set(%s.valueOf(keyPart));\n" % (prim)

        if i == 0:
            tmp = "\tif ( mKeyClzName.contains(\"%s\") ) {\n" % (line) 
            out.write(tmp)
            tmp = "\t\t%s tKey = (%s) mKey;\n" % (line, line)
            out.write(tmp)
            out.write(setStr)
        else:
            tmp = "\t} else if ( mKeyClzName.contains(\"%s\") ) {\n" % (line) 
            out.write(tmp)
            tmp = "\t\t%s tKey = (%s) mKey;\n" % (line, line)
            out.write(tmp)
            out.write(setStr)
    
        if i == size-1:
            out.write("\t}\n")
        else:
            out.write("\n")
        i=i+1
    out.write("\t//----------------------------------------------------------\n")

def write_partial_generic_proxy_mapper_valuepart(out,typeClasz):
    i = 0
    size = len(typeClasz)
    out.write("\t//----------------------------------------------------------\n")
    out.write("\t// Partial Generic Mapper Value Part\n")
    for line in typeClasz:
        prim = line.replace("Writable","")
        setStr = ""
        if prim == "Text" or prim == "UTF8":
            setStr = "\t\ttValue.set(nextToken)\n"
        elif prim == "Int":
            setStr = "\t\ttValue.set(Integer.valueOf(nextToken));\n"
        elif prim == "VInt":
            setStr = "\t\ttValue.set(Integer.valueOf(nextToken));\n"
        elif prim == "VLong":
            setStr = "\t\ttValue.set(Long.valueOf(nextToken));\n"
        else:
            setStr = "\t\ttValue.set(%s.valueOf(nextToken));\n" % (prim)

        if i == 0:
            tmp = "\tif ( mValueClzName.contains(\"%s\") ) {\n" % (line) 
            out.write(tmp)
            tmp = "\t\t%s tValue = (%s) mValue;\n" % (line, line)
            out.write(tmp)
            out.write(setStr)
        else:
            tmp = "\t} else if ( mValueClzName.contains(\"%s\") ) {\n" % (line) 
            out.write(tmp)
            tmp = "\t\t%s tValue = (%s) mValue;\n" % (line, line)
            out.write(tmp)
            out.write(setStr)
    
        if i == size-1:
            out.write("\t}\n")
        else:
            out.write("\n")
        i=i+1
    out.write("\t//----------------------------------------------------------\n")

def main(argv):
    print "argv.len: " + str(len(argv))
    if ( len(argv) < 3 ):
        print "!!! insufficient parameters"
        print "ProxySelectGenerator.py inputFile outputFile"
        sys.exit(1)
    print "input file " + argv[1]
    print "output file " + argv[2]
    #--------------------------------------
    # read file
    inf = open(argv[1],"r")
    typeClasz = []
    for line in inf:
        typeClasz.append(line.replace("\n",''))
    inf.close()
    #--------------------------------------
    ouf = open(argv[2],"w")
    write_add_proxy_mappers(ouf,typeClasz)
    write_add_proxy_reducers(ouf,typeClasz)
    write_proxy_mappers(ouf,typeClasz)
    write_proxy_reducers(ouf,typeClasz)
    write_partial_generic_proxy_mapper_keypart(ouf,typeClasz)
    write_partial_generic_proxy_mapper_valuepart(ouf,typeClasz)

    ouf.close()


if __name__ == "__main__":
    main(sys.argv)

