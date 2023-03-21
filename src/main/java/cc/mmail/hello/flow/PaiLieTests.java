package cc.mmail.hello.flow;

import java.util.Stack;

public class PaiLieTests {
    public static Stack<Integer> stack = new Stack<>();

    //示例:[1,2,3]
    //期望:[1,2,3],[1,3,2],[2,1,3],[2,3,1],[3,1,2],[3,2,1]
    public static void main(String[] args) {

        int[] arr = {1, 2, 3};

        f(arr, 3, 0);


    }

    private static void f(int[] arr, int tar, int cur) {
        if (tar == cur) {
            System.out.println(stack);
            return;
        }

        for (int i = 0; i < arr.length; i++) {
            if (!stack.contains(arr[i])) {
                stack.add(arr[i]);
                System.out.println(stack);
                System.out.println("开始调用f stack="+stack+"i="+i);
                f(arr, tar, cur + 1);
                stack.pop();
                System.out.println(stack);
            }
        }
        System.out.println(stack);
        System.out.println("调用结束");

    }
}
