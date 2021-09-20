abstract class Account{
    private String ownerName;
    protected double balance;
// initializing the constructor class
    Account(){
        this.ownerName = "jayakodi";
        this.balance = 0;
    }
    
    abstract void deposit(double amount);
// implementing the withdraw method
    public void withdraw(double amount){
        balance -= (amount + 5);
    }
    public String getOwnerName(){
        return ownerName;
    }
    
    public double getBalance(){
        return balance;
    }

    public void setOwnerName(String name){
        this.ownerName = name;
    }
}

class Isuru extends Account{
// implementation of the deposit method
    public void deposit(double amount){
        balance += amount + amount*0.05;
    }
}
class Nirogya extends Account{
// implementation of the deposit method
    public void deposit(double amount){
        balance += amount + amount*0.1;
    }
}
public class Main{
    public static void main(String []args){
    double amount = 1000;
    String name = "prasadi";
// creating a Isuru type object
    Isuru isuru = new Isuru();
    isuru.setOwnerName(name);// calling the both deposit and withdraw methods
    isuru.deposit(amount);
    isuru.withdraw(amount);
    System.out.println(isuru.getOwnerName());
    System.out.println(isuru.getBalance());
    }
}