package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by Molu on 28/4/17.
 */

public class Node implements Comparable<Node> {

    private String portNo;

    public String getHashValue() {
        return hashValue;
    }

    public void setHashValue(String hashValue) {
        this.hashValue = hashValue;
    }

    public String getPortNo() {
        return portNo;
    }

    public void setPortNo(String portNo) {
        this.portNo = portNo;
    }

    private String hashValue;

    public Node(String portNo,String hashValue)
    {
        this.portNo = portNo;
        this.hashValue = hashValue;
    }

    @Override
    public int compareTo(Node another) {
        return this.hashValue.compareTo(another.getHashValue());
    }

    @Override
    public boolean equals(Object another)
    {
        Node anotherNode = (Node) another;

        if(this.getPortNo().equals(anotherNode.getPortNo()))
            return true;

        return false;
    }
}
